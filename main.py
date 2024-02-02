from eventsourcing.application import Application
from eventsourcing.domain import Aggregate, event
from eventsourcing.utils import Environment


class DogSchool(Application):
    def register_dog(self, name):
        dog = Dog(name)
        self.save(dog)
        return dog.id

    def add_trick(self, dog_id, trick):
        dog = self.repository.get(dog_id)
        dog.add_trick(trick=trick)
        self.save(dog)

    def get_dog(self, dog_id):
        dog = self.repository.get(dog_id)
        return {'name': dog.name, 'tricks': tuple(dog.tricks)}


class Dog(Aggregate):
    @event('Registered')
    def __init__(self, name):
        self.name = name
        self.tricks = []

    @event('TrickAdded')
    def add_trick(self, trick):
        self.tricks.append(trick)


if __name__ == '__main__':

    environ = Environment()
    environ["PERSISTENCE_MODULE"] = "poc.postgres"
    # environ["PERSISTENCE_MODULE"] = "eventsourcing.postgres"
    environ["POSTGRES_HOST"] = "localhost"
    environ["POSTGRES_USER"] = "poc"
    environ["POSTGRES_DBNAME"] = "poc"
    environ["POSTGRES_PASSWORD"] = "poc"
    environ["TABLE_INDEX"] = "1"

    app = DogSchool(env=environ)

    limit = 1
    for i in range(limit):
        # dog_id = app.register_dog(name='Fido')
        # app.add_trick(dog_id, trick='roll over')
        # app.add_trick(dog_id, trick='fetch ball')

        dog1 = app.get_dog("565684a6-714c-4130-a990-e4c3fdd9021d")
        dog2 = app.get_dog("51bbbdf1-446d-490d-9a10-fd26b6422c34")
        print(dog1)
        print(dog2)

