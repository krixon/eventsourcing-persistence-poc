from __future__ import annotations

from itertools import chain
from threading import Lock
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
)
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg2
import psycopg2.errors
import psycopg2.extras
from psycopg2.errorcodes import DUPLICATE_PREPARED_STATEMENT

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    InterfaceError,
    Notification,
    OperationalError,
    ProcessRecorder,
    ProgrammingError,
    StoredEvent,
    Tracking,
)
from eventsourcing.utils import Environment, retry, strtobool
from eventsourcing.postgres import (
    PostgresCursor,
    PostgresConnection,
    PostgresDatastore,
    PG_IDENTIFIER_MAX_LEN
)

psycopg2.extras.register_uuid()


# There is a risk that the events table exceeds the limit for a single table.
# This limit is the number of rows which fits inside 4B pages.
# Need to determine the average row size.
# When the limit is reached, a new table is created and new events are stored here.
# Reads join across both tables to produce complete event streams.
# The last table is locked for writes, so that the notification_id is monotonic.
# Only the last table must be locked since no writes occur to older tables.
# The logic in this module handles inserting into the right table, and reading from both tables.


class PostgresAggregateRecorder(AggregateRecorder):
    def __init__(self, datastore: PostgresDatastore, events_table_name: str, events_table_index: int = 0):

        self.statement_name_aliases: Dict[str, str] = {}
        self.statement_name_aliases_lock = Lock()
        self.datastore = datastore

        self.events_table_base_name = events_table_name

        # The names of all events tables.
        # A table is created for each index value.
        # Events are read by joining across all tables, and are written to the last table (aka the current table).
        # In this way, there are no limits on the number of events that can be stored since new tables are created
        # when the current table is full. Full means that the number of rows exceeds that which will fit into
        # 4B heap pages. This is likely to be 10Bs of rows, so may never actually happen!
        self.events_table_names = [
            f"{events_table_name}_{i}" if i > 0 else events_table_name for i in range(events_table_index + 1)
        ]

        # The name of the current events table (i.e. the table that events are written to).
        self.events_table_name = self.events_table_names[-1]

        # The index of the current events table (i.e. the table that events are written to).
        # self.events_table_index = events_table_index

        self.lock_statements: List[str] = []

        self.insert_events_statement = f"INSERT INTO {self.events_table_name} VALUES ($1, $2, $3, $4)"
        self.insert_events_statement_name = f"insert_{self.events_table_name}".replace(".", "_")

        # Events are read across all events tables.
        selects = [f"SELECT * FROM {t} WHERE originator_id = $1" for t in self.events_table_names]
        self.select_events_statement = " UNION ALL ".join(selects)


    def insert_events(self, stored_events: List[StoredEvent], **kwargs: Any) -> Optional[Sequence[int]]:
        with self.datastore.get_connection() as conn:
            self._prepare_insert_events(conn)
            with conn.transaction(commit=True) as curs:
                return self._insert_events(curs, stored_events, **kwargs)

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def select_events(
        self,
        originator_id: UUID,
        gt: Optional[int] = None,
        lte: Optional[int] = None,
        desc: bool = False,
        limit: Optional[int] = None,
    ) -> List[StoredEvent]:
        parts = [self.select_events_statement]
        params: List[Any] = [originator_id]
        statement_name = f"select_{self.events_table_name}".replace(".", "_")
        if gt is not None:
            params.append(gt)
            parts.append(f"AND originator_version > ${len(params)}")
            statement_name += "_gt"
        if lte is not None:
            params.append(lte)
            parts.append(f"AND originator_version <= ${len(params)}")
            statement_name += "_lte"
        parts.append("ORDER BY originator_version")
        if desc is False:
            parts.append("ASC")
        else:
            parts.append("DESC")
            statement_name += "_desc"
        if limit is not None:
            params.append(limit)
            parts.append(f"LIMIT ${len(params)}")
            statement_name += "_limit"
        statement = " ".join(parts)

        stored_events = []

        with self.datastore.get_connection() as conn:
            alias = self._prepare(conn, statement_name, statement)

            with conn.transaction(commit=False) as curs:
                curs.execute(
                    f"EXECUTE {alias}({', '.join(['%s' for _ in params])})",
                    params,
                )
                for row in curs.fetchall():
                    stored_events.append(
                        StoredEvent(
                            originator_id=row["originator_id"],
                            originator_version=row["originator_version"],
                            topic=row["topic"],
                            state=bytes(row["state"]),
                        )
                    )
                pass  # for Coverage 5.5 bug with CPython 3.10.0rc1
            return stored_events

    def create_table(self) -> None:
        with self.datastore.transaction(commit=True) as curs:
            for statement in self._construct_create_table_statements():
                curs.execute(statement)

    def _construct_create_table_statements(self) -> List[str]:
        statement = (
            "CREATE TABLE IF NOT EXISTS "
            f"{self.events_table_name} ("
            "originator_id uuid NOT NULL, "
            "originator_version bigint NOT NULL, "
            "topic text, "
            "state bytea, "
            "PRIMARY KEY "
            "(originator_id, originator_version)) "
            "WITH (autovacuum_enabled=false)"
        )
        return [statement]

    def _prepare_insert_events(self, conn: PostgresConnection) -> None:
        self._prepare(
            conn,
            self.insert_events_statement_name,
            self.insert_events_statement,
        )

    def _prepare(self, conn: PostgresConnection, statement_name: str, statement: str) -> str:
        statement_name_alias = self._get_statement_alias(statement_name)
        if statement_name not in conn.is_prepared:
            curs: PostgresCursor
            with conn.transaction(commit=True) as curs:
                try:
                    lock_timeout = self.datastore.lock_timeout
                    curs.execute(f"SET LOCAL lock_timeout = '{lock_timeout}s'")
                    curs.execute(f"PREPARE {statement_name_alias} AS " + statement)
                except psycopg2.errors.lookup(DUPLICATE_PREPARED_STATEMENT):  # noqa
                    pass
                conn.is_prepared.add(statement_name)
        return statement_name_alias

    def _insert_events(
            self,
            c: PostgresCursor,
            stored_events: List[StoredEvent],
            **kwargs: Any,
    ) -> Optional[Sequence[int]]:
        # Acquire "EXCLUSIVE" table lock, to serialize inserts so that
        # insertion of notification IDs is monotonic for notification log
        # readers. We want concurrent transactions to commit inserted
        # notification_id values in order, and by locking the table for writes,
        # it can be guaranteed. The EXCLUSIVE lock mode does not block
        # the ACCESS SHARE lock which is acquired during SELECT statements,
        # so the table can be read concurrently. However, INSERT normally
        # just acquires ROW EXCLUSIVE locks, which risks interleaving of
        # many inserts in one transaction with many insert in another
        # transaction. Since one transaction will commit before another,
        # the possibility arises for readers that are tailing a notification
        # log to miss items inserted later but with lower notification IDs.
        # https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-TABLES
        # https://www.postgresql.org/docs/9.1/sql-lock.html
        # https://stackoverflow.com/questions/45866187/guarantee-monotonicity-of-postgresql-serial-column-values-by-commit-order

        len_stored_events = len(stored_events)

        # Only do something if there is something to do.
        if len_stored_events > 0:
            # Mogrify the table lock statements.
            lock_sqls = (c.mogrify(s) for s in self.lock_statements)

            # Prepare the commands before getting the table lock.
            alias = self.statement_name_aliases[self.insert_events_statement_name]
            page_size = 500

            pages = [
                (
                    c.mogrify(
                        f"EXECUTE {alias}(%s, %s, %s, %s)",
                        (
                            stored_event.originator_id,
                            stored_event.originator_version,
                            stored_event.topic,
                            stored_event.state,
                        ),
                    )
                    for stored_event in page
                )
                for page in (
                    stored_events[ndx: min(ndx + page_size, len_stored_events)]
                    for ndx in range(0, len_stored_events, page_size)
                )
            ]

            commands = [
                b"; ".join(page)
                for page in chain([chain(lock_sqls, pages[0])], pages[1:])
            ]

            for command in commands:
                c.execute(command)

        return None

    def _get_statement_alias(self, statement_name: str) -> str:
        try:
            alias = self.statement_name_aliases[statement_name]
        except KeyError:
            with self.statement_name_aliases_lock:
                try:
                    alias = self.statement_name_aliases[statement_name]
                except KeyError:
                    existing_aliases = self.statement_name_aliases.values()
                    if (
                            len(statement_name) <= PG_IDENTIFIER_MAX_LEN
                            and statement_name not in existing_aliases
                    ):
                        alias = statement_name
                        self.statement_name_aliases[statement_name] = alias
                    else:
                        uid = uuid5(
                            NAMESPACE_URL, f"/statement_names/{statement_name}"
                        ).hex
                        alias = uid
                        for i in range(len(uid)):  # pragma: no cover
                            preserve_end = 21
                            preserve_start = (
                                    PG_IDENTIFIER_MAX_LEN - preserve_end - i - 2
                            )
                            uuid5_tail = i
                            candidate = (
                                    statement_name[:preserve_start]
                                    + "_"
                                    + (uid[-uuid5_tail:] if i else "")
                                    + "_"
                                    + statement_name[-preserve_end:]
                            )
                            assert len(alias) <= PG_IDENTIFIER_MAX_LEN
                            if candidate not in existing_aliases:
                                alias = candidate
                                break
                        self.statement_name_aliases[statement_name] = alias
        return alias


class PostgresApplicationRecorder(PostgresAggregateRecorder, ApplicationRecorder):
    def __init__(
            self,
            datastore: PostgresDatastore,
            events_table_name: str = "stored_events",
            events_table_index: int = 0,
    ):
        super().__init__(datastore, events_table_name, events_table_index)

        self.notification_sequence_name = f"{self.events_table_base_name}_notification_id_seq"
        self.notification_id_index_name = f"{self.events_table_name}_notification_id_idx"

        self.insert_events_statement = (
            f"INSERT INTO {self.events_table_name} VALUES ($1, $2, $3, $4) RETURNING notification_id"
        )

        self.max_notification_id_statement = (
            f"SELECT MAX(notification_id) FROM {self.events_table_name}"
        )

        self.max_notification_id_statement_name = (
            f"max_notification_id_{events_table_name}".replace(".", "_")
        )

        self.lock_statements = [
            f"SET LOCAL lock_timeout = '{self.datastore.lock_timeout}s'",
            f"LOCK TABLE {self.events_table_name} IN EXCLUSIVE MODE",
        ]

    def _construct_create_table_statements(self) -> List[str]:
        statements = [
            f"CREATE SEQUENCE IF NOT EXISTS {self.notification_sequence_name} START 1;",

            "CREATE TABLE IF NOT EXISTS "
            f"{self.events_table_name} ("
            "originator_id uuid NOT NULL, "
            "originator_version bigint NOT NULL, "
            "topic text, "
            "state bytea, "
            f"notification_id bigint DEFAULT nextval('{self.notification_sequence_name}'), "
            "PRIMARY KEY "
            "(originator_id, originator_version)) "
            "WITH (autovacuum_enabled=false)",

            f"CREATE UNIQUE INDEX IF NOT EXISTS "
            f"{self.notification_id_index_name} "
            f"ON {self.events_table_name} (notification_id ASC);",
        ]
        return statements

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def select_notifications(
            self,
            start: int,
            limit: int,
            stop: Optional[int] = None,
            topics: Sequence[str] = (),
    ) -> List[Notification]:
        """
        Returns a list of event notifications
        from 'start', limited by 'limit'.
        """

        params: List[Union[int, str, Sequence[str]]] = [start]

        statement = (
            "SELECT * " f"FROM {self.events_table_name} " "WHERE notification_id>=$1 "
        )

        statement_name = f"select_notifications_{self.events_table_name}".replace(
            ".", "_"
        )

        if stop is not None:
            params.append(stop)
            statement += f"AND notification_id <= ${len(params)} "
            statement_name += "_stop"

        if topics:
            params.append(topics)
            statement += f"AND topic = ANY(${len(params)}) "
            statement_name += "_topics"

        params.append(limit)
        statement += "ORDER BY notification_id " f"LIMIT ${len(params)}"

        notifications = []
        with self.datastore.get_connection() as conn:
            alias = self._prepare(
                conn,
                statement_name,
                statement,
            )
            with conn.transaction(commit=False) as curs:
                curs.execute(
                    f"EXECUTE {alias}({', '.join(['%s' for _ in params])})",
                    params,
                )
                for row in curs.fetchall():
                    notifications.append(
                        Notification(
                            id=row["notification_id"],
                            originator_id=row["originator_id"],
                            originator_version=row["originator_version"],
                            topic=row["topic"],
                            state=bytes(row["state"]),
                        )
                    )
                pass  # for Coverage 5.5 bug with CPython 3.10.0rc1
        return notifications

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def max_notification_id(self) -> int:
        """
        Returns the maximum notification ID.
        """
        statement_name = self.max_notification_id_statement_name
        with self.datastore.get_connection() as conn:
            statement_alias = self._prepare(
                conn, statement_name, self.max_notification_id_statement
            )
            with conn.transaction(commit=False) as curs:
                curs.execute(
                    f"EXECUTE {statement_alias}",
                )
                max_id = curs.fetchone()[0] or 0
        return max_id

    def _insert_events(
            self,
            c: PostgresCursor,
            stored_events: List[StoredEvent],
            **kwargs: Any,
    ) -> Optional[Sequence[int]]:
        super()._insert_events(c, stored_events, **kwargs)
        if stored_events:
            last_notification_id = c.fetchone()[0]
            notification_ids = list(
                range(
                    last_notification_id - len(stored_events) + 1,
                    last_notification_id + 1,
                    )
            )
        else:
            notification_ids = []
        return notification_ids


class PostgresProcessRecorder(ProcessRecorder, PostgresApplicationRecorder):

    def max_tracking_id(self, application_name: str) -> int:
        pass

    def has_tracking_id(self, application_name: str, notification_id: int) -> bool:
        pass


class Factory(InfrastructureFactory):
    POSTGRES_DBNAME = "POSTGRES_DBNAME"
    POSTGRES_HOST = "POSTGRES_HOST"
    POSTGRES_PORT = "POSTGRES_PORT"
    POSTGRES_USER = "POSTGRES_USER"
    POSTGRES_PASSWORD = "POSTGRES_PASSWORD"
    POSTGRES_CONNECT_TIMEOUT = "POSTGRES_CONNECT_TIMEOUT"
    POSTGRES_CONN_MAX_AGE = "POSTGRES_CONN_MAX_AGE"
    POSTGRES_PRE_PING = "POSTGRES_PRE_PING"
    POSTGRES_POOL_TIMEOUT = "POSTGRES_POOL_TIMEOUT"
    POSTGRES_LOCK_TIMEOUT = "POSTGRES_LOCK_TIMEOUT"
    POSTGRES_POOL_SIZE = "POSTGRES_POOL_SIZE"
    POSTGRES_POOL_MAX_OVERFLOW = "POSTGRES_POOL_MAX_OVERFLOW"
    POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT = (
        "POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT"
    )
    POSTGRES_SCHEMA = "POSTGRES_SCHEMA"
    CREATE_TABLE = "CREATE_TABLE"
    TABLE_INDEX = "TABLE_INDEX"

    aggregate_recorder_class = PostgresAggregateRecorder
    application_recorder_class = PostgresApplicationRecorder
    process_recorder_class = PostgresProcessRecorder

    def __init__(self, env: Environment):
        super().__init__(env)
        dbname = self.env.get(self.POSTGRES_DBNAME)
        if dbname is None:
            raise EnvironmentError(
                "Postgres database name not found "
                "in environment with key "
                f"'{self.POSTGRES_DBNAME}'"
            )

        host = self.env.get(self.POSTGRES_HOST)
        if host is None:
            raise EnvironmentError(
                "Postgres host not found "
                "in environment with key "
                f"'{self.POSTGRES_HOST}'"
            )

        port = self.env.get(self.POSTGRES_PORT) or "5432"

        user = self.env.get(self.POSTGRES_USER)
        if user is None:
            raise EnvironmentError(
                "Postgres user not found "
                "in environment with key "
                f"'{self.POSTGRES_USER}'"
            )

        password = self.env.get(self.POSTGRES_PASSWORD)
        if password is None:
            raise EnvironmentError(
                "Postgres password not found "
                "in environment with key "
                f"'{self.POSTGRES_PASSWORD}'"
            )

        connect_timeout: Optional[int]
        connect_timeout_str = self.env.get(self.POSTGRES_CONNECT_TIMEOUT)
        if connect_timeout_str is None:
            connect_timeout = 5
        elif connect_timeout_str == "":
            connect_timeout = 5
        else:
            try:
                connect_timeout = int(connect_timeout_str)
            except ValueError:
                raise EnvironmentError(
                    f"Postgres environment value for key "
                    f"'{self.POSTGRES_CONNECT_TIMEOUT}' is invalid. "
                    f"If set, an integer or empty string is expected: "
                    f"'{connect_timeout_str}'"
                )

        idle_in_transaction_session_timeout_str = (
                self.env.get(self.POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT) or "5"
        )

        try:
            idle_in_transaction_session_timeout = int(
                idle_in_transaction_session_timeout_str
            )
        except ValueError:
            raise EnvironmentError(
                f"Postgres environment value for key "
                f"'{self.POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT}' is invalid. "
                f"If set, an integer or empty string is expected: "
                f"'{idle_in_transaction_session_timeout_str}'"
            )

        pool_size: Optional[int]
        pool_size_str = self.env.get(self.POSTGRES_POOL_SIZE)
        if pool_size_str is None:
            pool_size = 5
        elif pool_size_str == "":
            pool_size = 5
        else:
            try:
                pool_size = int(pool_size_str)
            except ValueError:
                raise EnvironmentError(
                    f"Postgres environment value for key "
                    f"'{self.POSTGRES_POOL_SIZE}' is invalid. "
                    f"If set, an integer or empty string is expected: "
                    f"'{pool_size_str}'"
                )

        pool_max_overflow: Optional[int]
        pool_max_overflow_str = self.env.get(self.POSTGRES_POOL_MAX_OVERFLOW)
        if pool_max_overflow_str is None:
            pool_max_overflow = 10
        elif pool_max_overflow_str == "":
            pool_max_overflow = 10
        else:
            try:
                pool_max_overflow = int(pool_max_overflow_str)
            except ValueError:
                raise EnvironmentError(
                    f"Postgres environment value for key "
                    f"'{self.POSTGRES_POOL_MAX_OVERFLOW}' is invalid. "
                    f"If set, an integer or empty string is expected: "
                    f"'{pool_max_overflow_str}'"
                )

        pool_timeout: Optional[float]
        pool_timeout_str = self.env.get(self.POSTGRES_POOL_TIMEOUT)
        if pool_timeout_str is None:
            pool_timeout = 30
        elif pool_timeout_str == "":
            pool_timeout = 30
        else:
            try:
                pool_timeout = float(pool_timeout_str)
            except ValueError:
                raise EnvironmentError(
                    f"Postgres environment value for key "
                    f"'{self.POSTGRES_POOL_TIMEOUT}' is invalid. "
                    f"If set, a float or empty string is expected: "
                    f"'{pool_timeout_str}'"
                )

        conn_max_age: Optional[float]
        conn_max_age_str = self.env.get(self.POSTGRES_CONN_MAX_AGE)
        if conn_max_age_str is None:
            conn_max_age = None
        elif conn_max_age_str == "":
            conn_max_age = None
        else:
            try:
                conn_max_age = float(conn_max_age_str)
            except ValueError:
                raise EnvironmentError(
                    f"Postgres environment value for key "
                    f"'{self.POSTGRES_CONN_MAX_AGE}' is invalid. "
                    f"If set, a float or empty string is expected: "
                    f"'{conn_max_age_str}'"
                )

        pre_ping = strtobool(self.env.get(self.POSTGRES_PRE_PING) or "no")

        lock_timeout_str = self.env.get(self.POSTGRES_LOCK_TIMEOUT) or "0"

        try:
            lock_timeout = int(lock_timeout_str)
        except ValueError:
            raise EnvironmentError(
                f"Postgres environment value for key "
                f"'{self.POSTGRES_LOCK_TIMEOUT}' is invalid. "
                f"If set, an integer or empty string is expected: "
                f"'{lock_timeout_str}'"
            )

        schema = self.env.get(self.POSTGRES_SCHEMA) or ""

        self.datastore = PostgresDatastore(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password,
            connect_timeout=connect_timeout,
            idle_in_transaction_session_timeout=idle_in_transaction_session_timeout,
            pool_size=pool_size,
            max_overflow=pool_max_overflow,
            pool_timeout=pool_timeout,
            conn_max_age=conn_max_age,
            pre_ping=pre_ping,
            lock_timeout=lock_timeout,
            schema=schema,
        )

        table_index_str = self.env.get(self.TABLE_INDEX) or "0"
        try:
            self.table_index = int(table_index_str)
        except ValueError:
            raise EnvironmentError(
                f"Postgres environment value for key "
                f"'{self.TABLE_INDEX}' is invalid. "
                f"If set, an integer or empty string is expected: "
                f"'{lock_timeout_str}'"
            )

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_" + purpose
        if self.datastore.schema:
            events_table_name = f"{self.datastore.schema}.{events_table_name}"
        recorder = type(self).aggregate_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
            events_table_index=self.table_index,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_events"
        if self.datastore.schema:
            events_table_name = f"{self.datastore.schema}.{events_table_name}"
        recorder = type(self).application_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
            events_table_index=self.table_index,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def process_recorder(self) -> ProcessRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_events"
        prefix = self.env.name.lower() or "notification"
        tracking_table_name = prefix + "_tracking"
        if self.datastore.schema:
            events_table_name = f"{self.datastore.schema}.{events_table_name}"
            tracking_table_name = f"{self.datastore.schema}.{tracking_table_name}"
        recorder = type(self).process_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
            events_table_index=self.table_index,
            tracking_table_name=tracking_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def env_create_table(self) -> bool:
        return strtobool(self.env.get(self.CREATE_TABLE) or "yes")

    def close(self) -> None:
        self.datastore.close()
