from c8.redis.hash_commands import HashCommands
from c8.redis.string_commands import StringCommands
from c8.redis.sorted_set_commands import SortedSetCommands
from c8.redis.list_commands import ListCommands
from c8.redis.set_commands import SetCommands


class Commands(
    StringCommands,
    HashCommands,
    SortedSetCommands,
    ListCommands,
    SetCommands
):
    pass
