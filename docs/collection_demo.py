from enum import Enum

class Animal(Enum):
    ant = 1
    bee = 2
    cat = 3
    dog = 4
class C(Animal):

    def enum(**enums):
        return type('Enum', (), enums)

    SPOT_CREATION_TYPES = enum(AUTOMATIC='automatic', NONE='none',
                               SPOT_REGION='spot_region')

    def ab(self, Animal.ant):
        SPO
