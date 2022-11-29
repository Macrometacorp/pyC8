from c8.executor import DefaultExecutor
from c8.redis.redis_interface import RedisInterface


class RedisCommands(object):
    def __init__(self, connection):
        self._conn = connection
        self._executor = DefaultExecutor(connection)

    def set(self, key, value, collection, options=[]):
        """
        Set key to hold the string value. If key already holds a value,
        it is overwritten, regardless of its type. Any previous time to live
        associated with the key is discarded on successful SET operation.
        More on https://redis.io/commands/set/

        :param key: Key of the data
        :type key: str
        :param value: Value of the data
        :type value: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Set options ex. [NX | XX] [GET] etc.
        :type options: list
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SET"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, value, *options
        )

    def append(self, key, value, collection):
        """
        If key already exists and is a string, this command appends the value at the
        end of the string. If key does not exist it is created and set as an empty
        string, so APPEND will be similar to SET in this special case.
        More on https://redis.io/commands/append/

        :param key: Key of the data
        :type key: str
        :param value: Value of the data
        :type value: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "APPEND"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, value
        )

    def decr(self, key, collection):
        """
        Decrements the number stored at key by one. If the key does not exist,
        it is set to 0 before performing the operation. An error is returned if the
        key contains a value of the wrong type or contains a string that can not be
        represented as integer. This operation is limited to 64 bit signed integers.
        More on https://redis.io/commands/decr/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "DECR"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key
        )

    def decrby(self, key, decrement, collection):
        """
        Decrements the number stored at key by decrement. If the key does not exist,
        it is set to 0 before performing the operation. An error is returned if the
        key contains a value of the wrong type or contains a string that can not be
        represented as integer. This operation is limited to 64 bit signed integers.
        More on https://redis.io/commands/decrby/

        :param key: Key of the data
        :type key: str
        :param decrement: Decrement number
        :type decrement: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "DECRBY"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, decrement
        )

    def get(self, key, collection):
        """
        Get the value of key. If the key does not exist the special value nil is
        returned. An error is returned if the value stored at key is not a string,
        because GET only handles string values.
        More on https://redis.io/commands/get/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "GET"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key
        )

    def getdel(self, key, collection):
        """
        Get the value of key and delete the key. This command is similar to GET,
        except for the fact that it also deletes the key on success (if and only if
        the key's value type is a string).
        More on https://redis.io/commands/getdel/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "GETDEL"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key
        )

    def getex(self, key, collection, expiry_command=None, time=None):
        """
        Get the value of key and optionally set its expiration. GETEX is similar to
        GET, but is a write command with additional options.
        More on https://redis.io/commands/getex/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param expiry_command: Redis expiry command (ex. EX, PX, EXAT, PXAT)
        :type expiry_command: str
        :param time: Redis expiry time (ex. sec, ms, unix-time-seconds etc.)
        :type time: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "GETEX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, expiry_command, time
        )

    def getrange(self, key, start, end, collection):
        """
        Returns the substring of the string value stored at key, determined by the
        offsets start and end (both are inclusive). Negative offsets can be used in
        order to provide an offset starting from the end of the string. So -1 means
        the last character, -2 the penultimate and so forth.
        The function handles out of range requests by limiting the resulting range to
        the actual length of the string.
        More on https://redis.io/commands/getrange/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param start: Start string offset
        :type start: int
        :param end: End string offset
        :type end: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "GETRANGE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, start, end
        )

    def getset(self, key, value, collection):
        """
        Atomically sets key to value and returns the old value stored at key. Returns
        an error when key exists but does not hold a string value. Any previous time
        to live associated with the key is discarded on successful SET operation.
        More on https://redis.io/commands/getset/

        :param key: Key of the data
        :type key: str
        :param value: Start string offset
        :type value: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "GETSET"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, value
        )

    def incr(self, key, collection):
        """
        Increments the number stored at key by one. If the key does not exist,
        it is set to 0 before performing the operation. An error is returned if the
        key contains a value of the wrong type or contains a string that can not be
        represented as integer. This operation is limited to 64 bit signed integers.
        More on https://redis.io/commands/incr/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "INCR"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key
        )

    def incrby(self, key, increment, collection):
        """
        Increments the number stored at key by increment. If the key does not exist,
        it is set to 0 before performing the operation. An error is returned if the
        key contains a value of the wrong type or contains a string that can not be
        represented as integer. This operation is limited to 64 bit signed integers.
        More on https://redis.io/commands/incrby/

        :param key: Key of the data
        :type key: str
        :param increment: Increment of the data
        :type increment: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "INCRBY"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, increment
        )

    def incrbyfloat(self, key, increment, collection):
        """
        Increment the string representing a floating point number stored at key by
        the specified increment. By using a negative increment value, the result is
        that the value stored at the key is decremented (by the obvious properties of
        addition). If the key does not exist, it is set to 0 before performing the
        operation. An error is returned if one of the following conditions occur:
        The key contains a value of the wrong type (not a string).
        The current key content or the specified increment are not parsable as a double
        precision floating point number.
        More on https://redis.io/commands/incrbyfloat/

        :param key: Key of the data
        :type key: str
        :param increment: Increment of the data
        :type increment: float
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "INCRBYFLOAT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, increment
        )

    def mget(self, keys, collection):
        """
        Returns the values of all specified keys. For every key that does not hold a
        string value or does not exist, the special value nil is returned. Because of
        this, the operation never fails.
        More on https://redis.io/commands/mget/

        :param keys: Keys of the data
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "MGET"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, *keys
        )

    def mset(self, data, collection):
        """
        Sets the given keys to their respective values. MSET replaces existing values
        with new values, just as regular SET. See MSETNX if you don't want to
        overwrite existing values.
        More on https://redis.io/commands/mset/

        :param data: Dictionary of the data
        :type data: dict
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "MSET"
        data_list = []
        for key, value in data.items():
            data_list.append(key)
            data_list.append(value)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, *data_list
        )

    def psetex(self, key, milliseconds, value, collection):
        """
        PSETEX works exactly like SETEX with the sole difference that the expire time
        is specified in milliseconds instead of seconds.
        More on https://redis.io/commands/psetex/

        :param key: Key of the data
        :type key: str
        :param milliseconds: TTL (time to leave) time of the data
        :type milliseconds: int
        :param value: Value of the data
        :type value: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "PSETEX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, milliseconds, value
        )

    def setbit(self, key, offset, value, collection):
        """
        Sets or clears the bit at offset in the string value stored at key.
        The bit is either set or cleared depending on value, which can be either 0 or 1.
        More on https://redis.io/commands/setbit/

        :param key: Key of the data
        :type key: str
        :param offset: Offset number
        :type offset: int
        :param value: Value of the data
        :type value: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SETBIT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, offset, value
        )

    def msetnx(self, data, collection):
        """
        Sets the given keys to their respective values. MSETNX will not perform any
        operation at all even if just a single key already exists.
        More on https://redis.io/commands/msetnx/

        :param data: Dictionary of the data
        :type data: dict
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "MSETNX"
        data_list = []
        for key, value in data.items():
            data_list.append(key)
            data_list.append(value)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, *data_list
        )

    def setex(self, key, seconds, value, collection):
        """
        Set key to hold the string value and set key to timeout after a given number of
        seconds.
        More on https://redis.io/commands/setex/

        :param key: Key of the data
        :type key: str
        :param seconds: TTL (time to leave) time of the data
        :type seconds: int
        :param value: Value of the data
        :type value: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SETEX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, seconds, value
        )

    def setnx(self, key, value, collection):
        """
        Set key to hold string value if key does not exist. In that case, it is equal to
        SET. When key already holds a value, no operation is performed.
        SETNX is short for "SET if Not eXists".
        More on https://redis.io/commands/setnx/

        :param key: Key of the data
        :type key: str
        :param value: Value of the data
        :type value: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SETNX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, value
        )

    def setrange(self, key, offset, value, collection):
        """
        Overwrites part of the string stored at key, starting at the specified
        offset, for the entire length of value. If the offset is larger than the
        current length of the string at key, the string is padded with zero-bytes to
        make offset fit. Non-existing keys are considered as empty strings, so this
        command will make sure it holds a string large enough to be able to set value
        at offset.
        More on https://redis.io/commands/setrange/

        :param key: Key of the data
        :type key: str
        :param offset: Offset of the data
        :type offset: int
        :param value: Value of the data
        :type value: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SETRANGE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, offset, value
        )

    def strlen(self, key, collection):
        """
        Returns the length of the string value stored at key.
        An error is returned when key holds a non-string value.
        More on https://redis.io/commands/strlen/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "STRLEN"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def bitcount(self, key, collection, start=None, end=None, data_format=None):
        """
        By default all the bytes contained in the string are examined. It is possible
        to specify the counting operation only in an interval passing the additional
        arguments start and end.
        More on https://redis.io/commands/bitcount/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param start: Count start
        :type start: int
        :param end: Count stop
        :type end: int
        :param data_format: Count format [BYTE | BIT]
        :type data_format: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "BITCOUNT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, start, end, data_format
        )

    def bitop(self, operation, deskey, keys, collection):
        """
        Perform a bitwise operation between multiple keys (containing string values) and
        store the result in the destination key.
        More on https://redis.io/commands/bitop/

        :param operation: Operation AND, OR, XOR and NOT
        :type operation: str
        :param deskey: Destination key where operation is stored
        :type deskey: str
        :param keys: List of keys to perform operation on
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "BITOP"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, operation, deskey, *keys
        )

    def bitpos(self, key, bit, collection, start=None, end=None, data_format=None):
        """
        Return the position of the first bit set to 1 or 0 in a string.
        The position is returned, thinking of the string as an array of bits from left
        to right, where the first byte's most significant bit is at position 0, the
        second byte's most significant bit is at position 8, and so forth.
        More on https://redis.io/commands/bitpos/

        :param key: Key of the data
        :type key: str
        :param bit: Bit value
        :type bit: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param start: Count start
        :type start: int
        :param end: Count stop
        :type end: int
        :param data_format: Count format [BYTE | BIT]
        :type data_format: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "BITPOS"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, bit, start, end, data_format
        )

    def getbit(self, key, offset, collection):
        """
        Returns the bit value at offset in the string value stored at key.
        More on https://redis.io/commands/getbit/

        :param key: Key of the data
        :type key: str
        :param offset: Offset of the data
        :type offset: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "GETBIT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
            offset,
        )

    def lpush(self, key, elements, collection):
        """
        Insert all the specified values at the head of the list stored at key. If key
        does not exist, it is created as empty list before performing the push
        operations. When key holds a value that is not a list, an error is returned.
        So for instance the command LPUSH mylist a b c will result into a list
        containing c as first element, b as second element and a as third element.
        More on https://redis.io/commands/lpush/

        :param key: Key of the data
        :type key: str
        :param elements: List of the data
        :type elements: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LPUSH"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *elements
        )

    def lindex(self, key, index, collection):
        """
        Returns the element at index index in the list stored at key. The index is
        zero-based, so 0 means the first element, 1 the second element and so on.
        Negative indices can be used to designate elements starting at the tail of
        the list. Here, -1 means the last element, -2 means the penultimate and so
        forth.
        More on https://redis.io/commands/lindex/

        :param key: Key of the data
        :type key: str
        :param index: Index of data
        :type index: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LINDEX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, index
        )

    def linsert(self, key, modifier, pivot, element, collection):
        """
        Inserts element in the list stored at key either before or after the reference
        value pivot.
        More on https://redis.io/commands/linsert/

        :param key: Key of the data
        :type key: str
        :param modifier: It can be BEFORE | AFTER
        :type modifier: str
        :param pivot: Pivot is reference value
        :type pivot: str
        :param element: New element to be added
        :type element: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LINSERT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, modifier, pivot, element
        )

    def llen(self, key, collection):
        """
        Returns the length of the list stored at key. If key does not exist,
        it is interpreted as an empty list and 0 is returned. An error is returned
        when the value stored at key is not a list.
        More on https://redis.io/commands/llen/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LLEN"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def lrange(self, key, start, stop, collection):
        """
        Returns the specified elements of the list stored at key. The offsets start
        and stop are zero-based indexes, with 0 being the first element of the list (
        the head of the list), 1 being the next element and so on. These offsets can
        also be negative numbers indicating offsets starting at the end of the list.
        For example, -1 is the last element of the list, -2 the penultimate,
        and so on.
        More on https://redis.io/commands/lrange/

        :param key: Key of the data
        :type key: str
        :param start: Start of the data
        :type start: int
        :param stop: Stop of the data
        :type stop: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LRANGE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, start, stop
        )

    def lmove(self, source, destination, where_from, where_to, collection):
        """
        Atomically returns and removes the first/last element (head/tail depending on
        the wherefrom argument) of the list stored at source, and pushes the element
        at the first/last element (head/tail depending on the whereto argument) of
        the list stored at destination.
        More on https://redis.io/commands/lmove/

        :param source: Source list
        :type source: str
        :param destination: Destination list
        :type destination: str
        :param where_from: From where to move <LEFT | RIGHT>
        :type where_from: str
        :param where_to: Position to move in <LEFT | RIGHT>
        :type where_to: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LMOVE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, source, destination, where_from, where_to
        )

    def lpos(self, key, element, collection, rank=None, count=None, max_len=None):
        """
        The command returns the index of matching elements inside a Redis list. By
        default, when no options are given, it will scan the list from head to tail,
        looking for the first match of "element". The optional arguments and options
        can modify the command's behavior. The RANK option specifies the "rank" of
        the first element to return, in case there are multiple matches. A rank of 1
        means to return the first match, 2 to return the second match, and so forth.
        Sometimes we want to return not just the Nth matching element, but the
        position of all the first N matching elements. This can be achieved using the
        COUNT option.
        Finally, the MAXLEN option tells the command to compare the provided element
        only with a given maximum number of list items
        More on https://redis.io/commands/lpos/

        :param key: Key of the data
        :type key: str
        :param element: Element to match
        :type element: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param rank: A rank of 1 means to return the first match, 2 to return the second
        :type rank: int
        :param count: count is the number of results
        :type count: int
        :param max_len: compare the element only with a maximum number of list items
        :type max_len: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LPOS"
        rank_list = []
        if rank is not None:
            rank_list.append("RANK")
            rank_list.append(rank)

        count_list = []
        if count is not None:
            count_list.append("COUNT")
            count_list.append(count)

        max_len_list = []
        if max_len is not None:
            max_len_list.append("MAXLEN")
            max_len_list.append(max_len)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, element, *rank_list, *count_list, *max_len_list
        )

    def rpush(self, key, elements, collection):
        """
        Insert all the specified values at the tail of the list stored at key. If key
        does not exist, it is created as empty list before performing the push
        operation. When key holds a value that is not a list, an error is returned.
        So for instance the command RPUSH mylist a b c will result into a list
        containing a as first element, b as second element and c as third element.
        More on https://redis.io/commands/rpush/

        :param key: Key of the data
        :type key: str
        :param elements: List of the data
        :type elements: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "RPUSH"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *elements
        )

    def lpop(
        self,
        key,
        collection,
        count=None,
    ):
        """
        Removes and returns the first elements of the list stored at key. By default,
        the command pops a single element from the beginning of the list. When
        provided with the optional count argument, the reply will consist of up to
        count elements, depending on the list's length.
        More on https://redis.io/commands/lpop/

        :param key: Key of the list
        :type key: str
        :param count: Count number of elements to pop
        :type count: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LPOP"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
            count,
        )

    def lpushx(self, key, elements, collection):
        """
        Inserts specified values at the head of the list stored at key, only if key
        already exists and holds a list. In contrary to LPUSH, no operation will be
        performed when key does not yet exist.
        More on https://redis.io/commands/lpushx/

        :param key: Key of the data
        :type key: str
        :param elements: List of the data
        :type elements: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LPUSHX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *elements
        )

    def rpushx(self, key, elements, collection):
        """
        Inserts specified values at the tail of the list stored at key, only if key
        already exists and holds a list. In contrary to RPUSH, no operation will be
        performed when key does not yet exist.
        More on https://redis.io/commands/rpushx/

        :param key: Key of the data
        :type key: str
        :param elements: List of the data
        :type elements: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "RPUSHX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *elements
        )

    def lrem(self, key, count, element, collection):
        """
        Removes the first count occurrences of elements equal to element from the list
        stored at key. The count argument influences the operation in the following
        ways:
        count > 0: Remove elements equal to element moving from head to tail.
        count < 0: Remove elements equal to element moving from tail to head.
        count = 0: Remove all elements equal to element.
        More on https://redis.io/commands/lrem/

        :param key: Key of the data
        :type key: str
        :param count: Number of elements to be removed
        :type count: int
        :param element: List element
        :type element: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LREM"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, count, element
        )

    def lset(self, key, index, element, collection):
        """
        Sets the list element at index to element. For more information on the index
        argument, see LINDEX.
        More on https://redis.io/commands/lset/

        :param key: Key of the data
        :type key: str
        :param index: Index of element
        :type index: int
        :param element: List element
        :type element: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LSET"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, index, element
        )

    def ltrim(self, key, start, stop, collection):
        """
        Trim an existing list so that it will contain only the specified range of
        elements specified. Both start and stop are zero-based indexes, where 0 is
        the first element of the list (the head), 1 the next element and so on.
        More on https://redis.io/commands/ltrim/

        :param key: Key of the data
        :type key: str
        :param start: Start index of element
        :type start: int
        :param stop: Stop index of element
        :type stop: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "LTRIM"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, start, stop
        )

    def rpop(
        self,
        key,
        collection,
        count=None,
    ):
        """
        Removes and returns the last elements of the list stored at key.
        By default, the command pops a single element from the end of the list. When
        provided with the optional count argument, the reply will consist of up to count
        elements, depending on the list's length.
        More on https://redis.io/commands/rpop/

        :param key: Key of the list
        :type key: str
        :param count: Count number of elements to pop
        :type count: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "RPOP"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
            count,
        )

    def rpoplpush(self, source, destination, collection):
        """
        Atomically returns and removes the last element (tail) of the list stored at
        source, and pushes the element at the first element (head) of the list stored
        at destination.
        More on https://redis.io/commands/rpoplpush/

        :param source: Source list
        :type source: str
        :param destination: Destination list
        :type destination: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "RPOPLPUSH"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            source,
            destination,
        )

    def hset(self, key, data, collection):
        """
        Sets field in the hash stored at key to value. If key does not exist,
        a new key holding a hash is created. If field already exists in the hash,
        it is overwritten.
        More on https://redis.io/commands/hset/

        :param key: Key of the data
        :type key: str
        :param data:  Dictionary of the data
        :type data: dict
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HSET"
        data_list = []
        for dict_key, dict_value in data.items():
            data_list.append(dict_key)
            data_list.append(dict_value)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *data_list
        )

    def hget(self, key, field, collection):
        """
        Returns the value associated with field in the hash stored at key.
        More on https://redis.io/commands/hget/

        :param key: Key of the data
        :type key: str
        :param field: Value of the data
        :type field: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HGET"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, field
        )

    def hdel(self, key, fields, collection):
        """
        Removes the specified fields from the hash stored at key. Specified fields
        that do not exist within this hash are ignored. If key does not exist,
        it is treated as an empty hash and this command returns 0
        More on https://redis.io/commands/hdel/

        :param key: Key of the data
        :type key: str
        :param fields: Fields of the data
        :type fields: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HDEL"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *fields
        )

    def hexists(self, key, field, collection):
        """
        Returns if field is an existing field in the hash stored at key.
        More on https://redis.io/commands/hexists/

        :param key: Key of the data
        :type key: str
        :param field: Field of the data
        :type field: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HEXISTS"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, field
        )

    def hgetall(self, key, collection):
        """
        Returns all fields and values of the hash stored at key. In the returned
        value, every field name is followed by its value, so the length of the reply
        is twice the size of the hash.
        More on https://redis.io/commands/hgetall/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HGETALL"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key
        )

    def hincrby(self, key, field, increment, collection):
        """
        Increments the number stored at field in the hash stored at key by increment.
        If key does not exist, a new key holding a hash is created. If field does not
        exist the value is set to 0 before the operation is performed.
        More on https://redis.io/commands/hincrby/

        :param key: Key of the data
        :type key: str
        :param field: Field of the data
        :type field: str
        :param increment: Increment number
        :type increment: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HINCRBY"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, field, increment
        )

    def hincrbyfloat(self, key, field, increment, collection):
        """
        Increment the specified field of a hash stored at key, and representing a
        floating point number, by the specified increment. If the increment value is
        negative, the result is to have the hash field value decremented instead of
        incremented. If the field does not exist, it is set to 0 before performing
        the operation. An error is returned if one of the following conditions occur:
        The field contains a value of the wrong type (not a string).
        The current field content or the specified increment are not parsable as a
        double precision floating point number.
        More on https://redis.io/commands/hincrbyfloat/

        :param key: Key of the data
        :type key: str
        :param field: Field of the data
        :type field: str
        :param increment: Increment number
        :type increment: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HINCRBYFLOAT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, field, increment
        )

    def hkeys(self, key, collection):
        """
        Returns all field names in the hash stored at key.
        More on https://redis.io/commands/hkeys/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HKEYS"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def hlen(self, key, collection):
        """
        Returns the number of fields contained in the hash stored at key.
        More on https://redis.io/commands/hlen/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HLEN"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def hmget(self, key, fields, collection):
        """
        Returns the values associated with the specified fields in the hash stored at
        key. For every field that does not exist in the hash, a nil value is returned.
        Because non-existing keys are treated as empty hashes, running HMGET against a
        non-existing key will return a list of nil values.
        More on https://redis.io/commands/hmget/

        :param key: Key of the data
        :type key: str
        :param fields: Fields of the data
        :type fields: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HMGET"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *fields
        )

    def hmset(self, key, data, collection):
        """
        Sets the specified fields to their respective values in the hash stored at
        key. This command overwrites any specified fields already existing in the
        hash. If key does not exist, a new key holding a hash is created. More on
        More on https://redis.io/commands/hmset/

        :param key: Key of the data
        :type key: str
        :param data: Dictionary of the data
        :type data: dict
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HMSET"
        data_list = []
        for dict_key, dict_value in data.items():
            data_list.append(dict_key)
            data_list.append(dict_value)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *data_list
        )

    def hscan(self, key, cursor, collection, pattern=None, count=None):
        """
        The SCAN command and the closely related commands SSCAN, HSCAN and ZSCAN are
        used in order to incrementally iterate over a collection of elements.
        More on https://redis.io/commands/scan/

        :param key: Key of the data
        :type key: str
        :param cursor: Cursor value (start with 0)
        :type cursor: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param pattern: It is possible to only iterate elements matching a given pattern
        :type pattern: str
        :param count: COUNT the user specified the amount of work that should be done
        :type count: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HSCAN"
        pattern_list = []
        if pattern is not None:
            pattern_list.append("MATCH")
            pattern_list.append(pattern)

        count_list = []
        if count is not None:
            count_list.append("COUNT")
            count_list.append(count)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, cursor, *pattern_list, *count_list
        )

    def hstrlen(self, key, field, collection):
        """
        Returns the string length of the value associated with field in the hash stored
        at key. If the key or the field do not exist, 0 is returned.
        More on https://redis.io/commands/hstrlen/

        :param key: Key of the data
        :type key: str
        :param field: Field of the data
        :type field: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HSTRLEN"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
            field,
        )

    def hrandfield(self, key, collection, count=None, modifier=None):
        """
        When called with just the key argument, return a random field from the hash
        value stored at key. f the provided count argument is positive, return an
        array of distinct fields. The array's length is either count or the hash's
        number of fields (HLEN), whichever is lower.
        The optional WITHVALUES modifier changes the reply so it includes the respective
        values of the randomly selected hash fields.
        More on https://redis.io/commands/hrandfield/

        :param key: Key of the data
        :type key: str
        :param count: Count of the data
        :type count: int
        :param modifier: The optional WITHVALUES modifier
        :type modifier: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HRANDFIELD"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
            count,
            modifier,
        )

    def hvals(self, key, collection):
        """
        Returns all values in the hash stored at key.
        More on https://redis.io/commands/hvals/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "HVALS"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def sadd(self, key, members, collection):
        """
        Add the specified members to the set stored at key. Specified members that
        are already a member of this set are ignored. If key does not exist,
        a new set is created before adding the specified members.
        More on https://redis.io/commands/sadd/

        :param key: Key of the data
        :type key: str
        :param members: list of members
        :type members: List
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SADD"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *members
        )

    def scard(self, key, collection):
        """
        Returns the set cardinality (number of elements) of the set stored at key.
        More on https://redis.io/commands/scard/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SCARD"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def sdiff(self, keys, collection):
        """
        Returns the members of the set resulting from the difference between the first
        set and all the successive sets.
        More on https://redis.io/commands/sdiff/

        :param keys: Key of the data
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SDIFF"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            *keys,
        )

    def sdiffstore(self, destination, keys, collection):
        """
        This command is equal to SDIFF, but instead of returning the resulting set, it
        is stored in destination.
        More on https://redis.io/commands/sdiffstore/

        :param destination: Key of the destination location
        :type destination: string
        :param keys: Key of the data
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SDIFFSTORE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            destination,
            *keys,
        )

    def sinter(self, keys, collection):
        """
        Returns the members of the set resulting from the intersection of all the given
        sets.
        More on https://redis.io/commands/sinter/

        :param keys: Key of the data
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SINTER"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            *keys,
        )

    def sinterstore(self, destination, keys, collection):
        """
        This command is equal to SINTER, but instead of returning the resulting set, it
        is stored in destination.
        More on https://redis.io/commands/sinterstore/

        :param destination: Key of the destination location
        :type destination: string
        :param keys: Key of the data
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SINTERSTORE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            destination,
            *keys,
        )

    def sismember(self, key, member, collection):
        """
        Returns if member is a member of the set stored at key.
        More on https://redis.io/commands/sismember/

        :param key: Key of the data
        :type key: str
        :param member: list of members
        :type member: string
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SISMEMBER"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, member
        )

    def smembers(self, key, collection):
        """
        Returns all the members of the set value stored at key.
        More on https://redis.io/commands/smembers/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SMEMBERS"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def smismember(self, key, members, collection):
        """
        Returns whether each member is a member of the set stored at key.
        For every member, 1 is returned if the value is a member of the set, or 0 if the
        element is not a member of the set or if key does not exist.
        More on https://redis.io/commands/smismember/

        :param key: Key of the data
        :type key: str
        :param members: list of members
        :type members: List
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SMISMEMBER"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *members
        )

    def smove(self, source, destination, member, collection):
        """
        Move member from the set at source to the set at destination. This operation is
        atomic. In every given moment the element will appear to be a member of source
        or destination for other clients.
        More on https://redis.io/commands/smove/

        :param source: Source set
        :type source: str
        :param destination: Destination set
        :type destination: str
        :param member: Member of the set to be moved
        :type member: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SMOVE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, source, destination, member
        )

    def spop(self, key, count, collection):
        """
        Removes and returns one or more random members from the set value store at key.
        This operation is similar to SRANDMEMBER, that returns one or more random
        elements from a set but does not remove it.
        More on https://redis.io/commands/spop/

        :param key: Key of the data
        :type key: str
        :param count: Count of the data
        :type count: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SPOP"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, count
        )

    def srandmember(self, key, collection, count=None):
        """
        When called with just the key argument, return a random element from the set
        value stored at key. If the provided count argument is positive, return an array
        of distinct elements. The array's length is either count or the set's
        cardinality (SCARD), whichever is lower.
        More on https://redis.io/commands/srandmember/

        :param key: Key of the data
        :type key: str
        :param count: Count of the data
        :type count: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SRANDMEMBER"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, count
        )

    def srem(self, key, members, collection):
        """
        Remove the specified members from the set stored at key. Specified members that
        are not a member of this set are ignored. If key does not exist, it is treated
        as an empty set and this command returns 0.
        More on https://redis.io/commands/srem/

        :param key: Key of the data
        :type key: str
        :param members: list of members
        :type members: List
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SREM"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *members
        )

    def sscan(self, key, cursor, collection, pattern=None, count=None):
        """
        The SCAN command and the closely related commands SSCAN, HSCAN and ZSCAN are
        used in order to incrementally iterate over a collection of elements.
        More on https://redis.io/commands/scan/

        :param key: Key of the data
        :type key: str
        :param cursor: Cursor value (start with 0)
        :type cursor: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param pattern: It is possible to only iterate elements matching a given pattern
        :type pattern: str
        :param count: COUNT the user specified the amount of work that should be done
        :type count: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SSCAN"
        pattern_list = []
        if pattern is not None:
            pattern_list.append("MATCH")
            pattern_list.append(pattern)

        count_list = []
        if count is not None:
            count_list.append("COUNT")
            count_list.append(count)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, cursor, *pattern_list, *count_list
        )

    def sunion(self, keys, collection):
        """
        Returns the members of the set resulting from the union of all the given sets.
        More on https://redis.io/commands/sunion/

        :param keys: Key of the data
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SUNION"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            *keys,
        )

    def sunionstore(self, destination, keys, collection):
        """
        This command is equal to SUNION, but instead of returning the resulting set, it
        is stored in destination.
        More on https://redis.io/commands/sunionstore/

        :param destination: Key of the destination location
        :type destination: string
        :param keys: Key of the data
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SUNIONSTORE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            destination,
            *keys,
        )

    def zadd(self, key, data, collection, options=[]):
        """
        Adds all the specified members with the specified scores to the sorted set
        stored at key. It is possible to specify multiple score / member pairs. If a
        specified member is already a member of the sorted set, the score is updated
        and the element reinserted at the right position to ensure the correct
        ordering.
        More on https://redis.io/commands/zadd/

        :param key: Key of the sorted set
        :type key: str
        :param data: List of score member data ex. [0, "test0", 1, "test1"]
        :type data: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Additional ZADD options [NX | XX] [GT | LT] [CH] [INCR]
        :type options: list
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZADD"

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *options, *data
        )

    def zcard(self, key, collection):
        """
        Returns the sorted set cardinality (number of elements) of the sorted set stored
        at key.
        More on https://redis.io/commands/zcard/

        :param key: Key of the sorted set
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZCARD"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def zcount(self, key, minimum, maximum, collection):
        """
        Returns the number of elements in the sorted set at key with a score between min
        and max.
        The min and max arguments have the same semantic as described for ZRANGEBYSCORE.
        More on https://redis.io/commands/zcount/

        :param key: Key of the data
        :type key: str
        :param minimum: Minimum score
        :type minimum: str
        :param maximum: Maximum score
        :type maximum: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZCOUNT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, minimum, maximum
        )

    def zdiff(self, num_keys, keys, collection, with_scores=False):
        """
        This command is similar to ZDIFFSTORE, but instead of storing the resulting
        sorted set, it is returned to the client.
        More on https://redis.io/commands/zdiff/

        :param num_keys: Total number of input keys
        :type num_keys: int
        :param keys: List of keys of the sorted set
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param with_scores: Return score of member
        :type with_scores: bool
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZDIFF"
        if with_scores is True:
            with_scores_command = "WITHSCORES"
        else:
            with_scores_command = None

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, num_keys, *keys, with_scores_command
        )

    def zdiffstore(self, destination, num_keys, keys, collection):
        """
        Computes the difference between the first and all successive input sorted
        sets and stores the result in destination. The total number of input keys is
        specified by numkeys.
        More on https://redis.io/commands/zdiffstore/

        :param destination: Destination key to store result
        :type destination: str
        :param num_keys: Total number of input keys
        :type num_keys: int
        :param keys: List of keys of the sorted set
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZDIFFSTORE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            destination,
            num_keys,
            *keys,
        )

    def zincrby(self, key, increment, member, collection):
        """
        Increments the score of member in the sorted set stored at key by increment.
        If member does not exist in the sorted set, it is added with increment as its
        score (as if its previous score was 0.0). If key does not exist, a new sorted
        set with the specified member as its sole member is created.
        More on https://redis.io/commands/zincrby/

        :param key: Key of the sorted set
        :type key: str
        :param increment: Value that increments score of the member
        :type increment: float
        :param member: Member to be incremented
        :type member: string
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZINCRBY"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, increment, member
        )

    def zinter(self, num_keys, keys, collection, options=None, with_scores=False):
        """
        This command is similar to ZINTERSTORE, but instead of storing the resulting
        sorted set, it is returned to the client.
        For a description of the WEIGHTS and AGGREGATE options, see ZUNIONSTORE.
        More on https://redis.io/commands/zinter/

        :param num_keys: Total number of input keys
        :type num_keys: int
        :param keys: List of keys of the sorted set
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Additional ZINTER options [WEIGHTS weight [weight ...]]
        [AGGREGATE <SUM | MIN | MAX>]
        :type options: list
        :param with_scores: Return score of member
        :type with_scores: bool
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZINTER"
        options_command = []
        if options is not None:
            options_command = list(options)

        if with_scores is True:
            options_command.append("WITHSCORES")

        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            num_keys,
            *keys,
            *options_command,
        )

    def zinterstore(self, destination, num_keys, keys, collection, options=[]):
        """
        Computes the intersection of numkeys sorted sets given by the specified keys,
        and stores the result in destination. It is mandatory to provide the number
        of input keys (numkeys) before passing the input keys and the other (
        optional) arguments. For a description of the WEIGHTS and AGGREGATE options,
        see ZUNIONSTORE.
        More on https://redis.io/commands/zinterstore/

        :param destination: Destination key to store result
        :type destination: str
        :param num_keys: Total number of input keys
        :type num_keys: int
        :param keys: List of keys of the sorted set
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Additional ZINTER options [WEIGHTS weight [weight ...]]
        [AGGREGATE <SUM | MIN | MAX>]
        :type options: list
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZINTERSTORE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            destination,
            num_keys,
            *keys,
            *options,
        )

    def zlexcount(self, key, minimum, maximum, collection):
        """
        When all the elements in a sorted set are inserted with the same score,
        in order to force lexicographical ordering, this command returns the number
        of elements in the sorted set at key with a value between min and max.
        The min and max arguments have the same meaning as described for ZRANGEBYLEX.
        More on https://redis.io/commands/zlexcount/

        :param key: Key of the data
        :type key: str
        :param minimum: Minimum score
        :type minimum: str
        :param maximum: Maximum score
        :type maximum: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZLEXCOUNT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, minimum, maximum
        )

    def zmscore(self, key, members, collection):
        """
        Returns the scores associated with the specified members in the sorted set
        stored at key.
        More on https://redis.io/commands/zmscore/

        :param key: Key of the sorted set
        :type key: str
        :param members: Members list of the sorted set
        :type members: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZMSCORE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *members
        )

    def zpopmax(self, key, collection, count=None):
        """
        Removes and returns up to count members with the highest scores in the sorted
        set stored at key. When left unspecified, the default value for count is 1.
        More on https://redis.io/commands/zpopmax/

        :param key: Key of the sorted set
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param count: Number of elements to be removed
        :type count: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZPOPMAX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, count
        )

    def zpopmin(self, key, collection, count=None):
        """
        Removes and returns up to count members with the lowest scores in the sorted set
        stored at key.
        When left unspecified, the default value for count is 1
        More on https://redis.io/commands/zpopmin/

        :param key: Key of the sorted set
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param count: Number of elements to be removed
        :type count: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZPOPMIN"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, count
        )

    def zrandmember(self, key, collection, count=None, with_scores=False):
        """
        When called with just the key argument, return a random element from the
        sorted set value stored at key. If the provided count argument is positive,
        return an array of distinct elements. The array's length is either count or
        the sorted set's cardinality (ZCARD), whichever is lower.
        More on https://redis.io/commands/zrandmember/

        :param key: Key of the sorted set
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param count: Number of elements to be removed
        :type count: int
        :param with_scores: Return score of member
        :type with_scores: bool
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZRANDMEMBER"
        if with_scores is True:
            with_scores_command = "WITHSCORES"
        else:
            with_scores_command = None

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, count, with_scores_command
        )

    def zrange(self, key, start, stop, collection, options=[]):
        """
        Returns the specified range of elements in the sorted set stored at <key>.
        ZRANGE can perform different types of range queries: by index (rank), by the
        score, or by lexicographical order.
        More on https://redis.io/commands/zrange/

        :param key: Key of the sorted set
        :type key: str
        :param start: Start of the data
        :type start: str
        :param stop: Stop of the data
        :type stop: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Additional ZRANGE options ex. [BYSCORE | BYLEX] [REV] etc.
        :type options: list
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZRANGE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, start, stop, *options
        )

    def zrangebylex(self, key, minimum, maximum, collection, offset=None, count=None):
        """When all the elements in a sorted set are inserted with the same score,
        in order to force lexicographical ordering, this command returns all the
        elements in the sorted set at key with a value between min and max.
        If the elements in the sorted set have different scores, the returned elements
        are unspecified.
        The optional LIMIT argument can be used to only get a range of the matching
        elements (similar to SELECT LIMIT offset, count in SQL)
        More on https://redis.io/commands/zrangebylex/

        :param key: Key of the data
        :type key: str
        :param minimum: Minimum score
        :type minimum: str
        :param maximum: Maximum score
        :type maximum: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param offset: Offset of the limit parameter
        :type offset: str
        :param count: Count of the limit parameter
        :type count: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        limit_list = []
        if offset and count is not None:
            limit_list.append("LIMIT")
            limit_list.append(offset)
            limit_list.append(count)

        command = "ZRANGEBYLEX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, minimum, maximum, *limit_list
        )

    def zrangebyscore(
        self,
        key,
        minimum,
        maximum,
        collection,
        with_scores=None,
        offset=None,
        count=None,
    ):
        """When all the elements in a sorted set are inserted with the same score,
        in order to force lexicographical ordering, this command returns all the
        elements in the sorted set at key with a value between min and max.
        If the elements in the sorted set have different scores, the returned elements
        are unspecified.
        The optional LIMIT argument can be used to only get a range of the matching
        elements (similar to SELECT LIMIT offset, count in SQL)
        More on https://redis.io/commands/zrangebyscore/

        :param key: Key of the data
        :type key: str
        :param minimum: Minimum score
        :type minimum: str
        :param maximum: Maximum score
        :type maximum: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param with_scores: Return score of member
        :type with_scores: bool
        :param offset: Offset of the limit parameter
        :type offset: str
        :param count: Count of the limit parameter
        :type count: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZRANGEBYSCORE"
        if with_scores is True:
            with_scores_command = "WITHSCORES"
        else:
            with_scores_command = None

        limit_list = []
        if offset and count is not None:
            limit_list.append("LIMIT")
            limit_list.append(offset)
            limit_list.append(count)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, minimum, maximum, with_scores_command, *limit_list
        )

    def zrangestore(self, dst, key, minimum, maximum, collection, options=[]):
        """
        This command is like ZRANGE, but stores the result in the <dst> destination key.
        More on https://redis.io/commands/zrangestore/

        :param dst: Key of the destination location
        :type dst: string
        :param key: Key of the sorted set
        :type key: str
        :param minimum: Start of the data
        :type minimum: str
        :param maximum: Stop of the data
        :type maximum: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Additional ZRANGE options ex. [BYSCORE | BYLEX] [REV] etc.
        :type options: list
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZRANGESTORE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, dst, key, minimum, maximum, *options
        )

    def zrank(self, key, member, collection):
        """
        Returns the rank of member in the sorted set stored at key, with the scores
        ordered from low to high. The rank (or index) is 0-based, which means that
        the member with the lowest score has rank 0.
        More on https://redis.io/commands/zrank/

        :param key: Key of the sorted set
        :type key: str
        :param member: Member of the sorted set
        :type member: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZRANK"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, member
        )

    def zrem(self, key, members, collection):
        """
        Removes the specified members from the sorted set stored at key. Non existing
        members are ignored.
        An error is returned when key exists and does not hold a sorted set.
        More on https://redis.io/commands/zrem/

        :param key: Key of the sorted set
        :type key: str
        :param members: List of members of the sorted set to be removed
        :type members: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZREM"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, *members
        )

    def zremrangebylex(
        self,
        key,
        minimum,
        maximum,
        collection,
    ):
        """
        When all the elements in a sorted set are inserted with the same score,
        in order to force lexicographical ordering, this command removes all elements
        in the sorted set stored at key between the lexicographical range specified
        by min and max.
        The meaning of min and max are the same of the ZRANGEBYLEX command.
        More on https://redis.io/commands/zremrangebylex/

        :param key: Key of the sorted set
        :type key: str
        :param minimum: Minimum parameter of the data
        :type minimum: str
        :param maximum: Maximum parameter of the data
        :type maximum: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZREMRANGEBYLEX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, minimum, maximum
        )

    def zremrangebyrank(
        self,
        key,
        start,
        stop,
        collection,
    ):
        """
        Removes all elements in the sorted set stored at key with rank between start
        and stop. Both start and stop are 0 -based indexes with 0 being the element
        with the lowest score. These indexes can be negative numbers, where they
        indicate offsets starting at the element with the highest score. For example:
        -1 is the element with the highest score, -2 the element with the second
        highest score and so forth.
        More on https://redis.io/commands/zremrangebyrank/

        :param key: Key of the sorted set
        :type key: str
        :param start: Start of the data
        :type start: str
        :param stop: Stop of the data
        :type stop: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZREMRANGEBYRANK"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, start, stop
        )

    def zremrangebyscore(
        self,
        key,
        minimum,
        maximum,
        collection,
    ):
        """
        Removes all elements in the sorted set stored at key with a score between min
        and max (inclusive).
        More on https://redis.io/commands/zremrangebyscore/

        :param key: Key of the sorted set
        :type key: str
        :param minimum: Minimum parameter of the data
        :type minimum: str
        :param maximum: Maximum parameter of the data
        :type maximum: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZREMRANGEBYSCORE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, minimum, maximum
        )

    def zrevrange(self, key, start, stop, collection, with_scores=False):
        """
        Returns the specified range of elements in the sorted set stored at key. The
        elements are considered to be ordered from the highest to the lowest score.
        Descending lexicographical order is used for elements with equal score.
        Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE.
        More on https://redis.io/commands/zrevrange/

        :param key: Key of the sorted set
        :type key: str
        :param start: Start of the data
        :type start: int
        :param stop: Stop of the data
        :type stop: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param with_scores: Return score of member
        :type with_scores: bool
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZREVRANGE"

        if with_scores is True:
            with_scores_command = "WITHSCORES"
        else:
            with_scores_command = None

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, start, stop, with_scores_command
        )

    def zrevrangebylex(
        self, key, minimum, maximum, collection, offset=None, count=None
    ):
        """When all the elements in a sorted set are inserted with the same score,
        in order to force lexicographical ordering, this command returns all the
        elements in the sorted set at key with a value between max and min.
        Apart from the reversed ordering, ZREVRANGEBYLEX is similar to ZRANGEBYLEX.
        The optional LIMIT argument can be used to only get a range of the matching
        elements (similar to SELECT LIMIT offset, count in SQL) More on
        https://redis.io/commands/zrevrangebylex/

        :param key: Key of the data
        :type key: str
        :param minimum: Minimum score
        :type minimum: str
        :param maximum: Maximum score
        :type maximum: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param offset: Offset of the limit parameter
        :type offset: str
        :param count: Count of the limit parameter
        :type count: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZREVRANGEBYLEX"
        limit_list = []
        if offset and count is not None:
            limit_list.append("LIMIT")
            limit_list.append(offset)
            limit_list.append(count)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, minimum, maximum, *limit_list
        )

    def zrevrangebyscore(
        self,
        key,
        minimum,
        maximum,
        collection,
        with_scores=None,
        offset=None,
        count=None,
    ):
        """Returns all the elements in the sorted set at key with a score between max
        and min (including elements with score equal to max or min). In contrary to
        the default ordering of sorted sets, for this command the elements are
        considered to be ordered from high to low scores.
        Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to ZRANGEBYSCORE.
        The optional LIMIT argument can be used to only get a range of the matching
        elements (similar to SELECT LIMIT offset, count in SQL)
        More on https://redis.io/commands/zrevrangebyscore/

        :param key: Key of the data
        :type key: str
        :param minimum: Minimum score
        :type minimum: str
        :param maximum: Maximum score
        :type maximum: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param with_scores: Return score of member
        :type with_scores: bool
        :param offset: Offset of the limit parameter
        :type offset: str
        :param count: Count of the limit parameter
        :type count: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZREVRANGEBYSCORE"
        if with_scores is True:
            with_scores_command = "WITHSCORES"
        else:
            with_scores_command = None

        limit_list = []
        if offset and count is not None:
            limit_list.append("LIMIT")
            limit_list.append(offset)
            limit_list.append(count)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, minimum, maximum, with_scores_command, *limit_list
        )

    def zrevrank(self, key, member, collection):
        """
        Returns the rank of member in the sorted set stored at key, with the scores
        ordered from high to low. The rank (or index) is 0-based, which means that
        the member with the highest score has rank 0.
        More on https://redis.io/commands/zrevrank/

        :param key: Key of the sorted set
        :type key: str
        :param member: Member of the sorted set
        :type member: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZREVRANK"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, member
        )

    def zscan(self, key, cursor, collection, pattern=None, count=None):
        """
        The SCAN command and the closely related commands SSCAN, HSCAN and ZSCAN are
        used in order to incrementally iterate over a collection of elements.
        More on https://redis.io/commands/scan/

        :param key: Key of the data
        :type key: str
        :param cursor: Cursor value (start with 0)
        :type cursor: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param pattern: It is possible to only iterate elements matching a given pattern
        :type pattern: str
        :param count: COUNT the user specified the amount of work that should be done
        :type count: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZSCAN"
        pattern_list = []
        if pattern is not None:
            pattern_list.append("MATCH")
            pattern_list.append(pattern)

        count_list = []
        if count is not None:
            count_list.append("COUNT")
            count_list.append(count)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, cursor, *pattern_list, *count_list
        )

    def zscore(self, key, member, collection):
        """
        Returns the sorted set cardinality (number of elements) of the sorted set stored
        at key.
        More on https://redis.io/commands/zscore/

        :param key: Key of the sorted set
        :type key: str
        :param member: Member of the sorted set
        :type member: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZSCORE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, member
        )

    def zunion(self, num_keys, keys, collection, options=None, with_scores=False):
        """
        This command is similar to ZUNIONSTORE, but instead of storing the resulting
        sorted set, it is returned to the client. For a description of the WEIGHTS
        and AGGREGATE options, see ZUNIONSTORE.
        More on https://redis.io/commands/zunion/

        :param num_keys: Total number of input keys
        :type num_keys: int
        :param keys: List of keys of the sorted set
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Additional ZUNION options ex. [WEIGHTS weight [weight ...]] etc.
        :type options: list
        :param with_scores: Return score of member
        :type with_scores: bool
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZUNION"
        options_command = []
        if options is not None:
            options_command = list(options)

        if with_scores is True:
            options_command.append("WITHSCORES")

        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            num_keys,
            *keys,
            *options_command,
        )

    def zunionstore(
        self, destination, num_keys, keys, collection, options=None, with_scores=False
    ):
        """
        Computes the union of numkeys sorted sets given by the specified keys,
        and stores the result in destination. It is mandatory to provide the number
        of input keys (numkeys) before passing the input keys and the other (
        optional) arguments.
        By default, the resulting score of an element is the sum of its scores in the
        sorted sets where it exists.
        More on https://redis.io/commands/zunionstore/

        :param destination: Destination sorted set
        :type destination: str
        :param num_keys: Total number of input keys
        :type num_keys: int
        :param keys: List of keys of the sorted set
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Additional ZUNIONSTORE options
        :type options: list
        :param with_scores: Return score of member
        :type with_scores: bool
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ZUNIONSTORE"
        options_command = []
        if options is not None:
            options_command = list(options)

        if with_scores is True:
            options_command.append("WITHSCORES")

        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            destination,
            num_keys,
            *keys,
            *options_command,
        )

    def copy(
        self, source, destination, collection, destination_database=None, replace=False
    ):
        """
        This command copies the value stored at the source key to the destination
        key. By default, the destination key is created in the logical database used
        by the connection. The DB option allows specifying an alternative logical
        database index for the destination key.
        More on https://redis.io/commands/copy/

        :param source: Source key to be copied
        :type source: str
        :param destination: Destination key to be copied
        :type destination: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param destination_database: DB location where data will be copied
        :type destination_database: str
        :param replace: Replace removes destination key before copying value to it
        :type replace: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "COPY"
        options_command = []
        if destination_database is not None:
            options_command.append("DB")
            options_command.append(destination_database)

        if replace is True:
            options_command.append("WITHSCORES")

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, source, destination, *options_command
        )

    def delete(
        self,
        keys,
        collection,
    ):
        """
        Removes the specified keys. A key is ignored if it does not exist.
        More on https://redis.io/commands/del/

        :param keys: List of keys to be deleted
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "DEL"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, *keys
        )

    def exists(
        self,
        keys,
        collection,
    ):
        """
        Returns if key exists.
        More on https://redis.io/commands/exists/

        :param keys: List of keys to be checked
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "EXISTS"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, *keys
        )

    def expire(
        self,
        key,
        seconds,
        collection,
        options=None,
    ):
        """
        Set a timeout on key. After the timeout has expired, the key will
        automatically be deleted. A key with an associated timeout is often said to
        be volatile in Redis terminology.
        More on https://redis.io/commands/expire/

        :param key: Key of the data
        :type key: str
        :param seconds: Time until key expires
        :type seconds: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Options of expire command [NX | XX | GT | LT]
        :type options: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "EXPIRE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, seconds, options
        )

    def expireat(
        self,
        key,
        unix_time_seconds,
        collection,
        options=None,
    ):
        """
        EXPIREAT has the same effect and semantic as EXPIRE, but instead of
        specifying the number of seconds representing the TTL (time to live),
        it takes an absolute Unix timestamp (seconds since January 1, 1970). A
        timestamp in the past will delete the key immediately.
        More on https://redis.io/commands/expireat/

        :param key: Key of the data
        :type key: str
        :param unix_time_seconds: Time until key expires
        :type unix_time_seconds: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Options of expire command [NX | XX | GT | LT]
        :type options: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "EXPIREAT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, unix_time_seconds, options
        )

    def persist(self, key, collection):
        """
        Remove the existing timeout on key, turning the key from volatile (a key with
        an expire set) to persistent (a key that will never expire as no timeout is
        associated).
        More on https://redis.io/commands/persist/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "PERSIST"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def pexpire(
        self,
        key,
        milliseconds,
        collection,
        options=None,
    ):
        """
        EXPIREAT has the same effect and semantic as EXPIRE, but instead of
        specifying the number of seconds representing the TTL (time to live),
        it takes an absolute Unix timestamp (seconds since January 1, 1970). A
        timestamp in the past will delete the key immediately.
        More on https://redis.io/commands/pexpire/

        :param key: Key of the data
        :type key: str
        :param milliseconds: Time until key expires
        :type milliseconds: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Options of expire command [NX | XX | GT | LT]
        :type options: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "PEXPIRE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, milliseconds, options
        )

    def pexpireat(
        self,
        key,
        unix_time_milliseconds,
        collection,
        options=None,
    ):
        """
        PEXPIREAT has the same effect and semantic as EXPIREAT, but the Unix time at
        which the key will expire is specified in milliseconds instead of seconds.
        More on https://redis.io/commands/pexpireat/

        :param key: Key of the data
        :type key: str
        :param unix_time_milliseconds: Time until key expires
        :type unix_time_milliseconds: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param options: Options of expire command [NX | XX | GT | LT]
        :type options: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "PEXPIREAT"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, unix_time_milliseconds, options
        )

    def pttl(
        self,
        key,
        collection,
    ):
        """
        Like TTL this command returns the remaining time to live of a key that has an
        expire set, with the sole difference that TTL returns the amount of remaining
        time in seconds while PTTL returns it in milliseconds.
        More on https://redis.io/commands/pttl/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "PTTL"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def randomkey(
        self,
        collection,
    ):
        """
        Return a random key from the currently selected database.
        More on https://redis.io/commands/randomkey/

        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "RANDOMKEY"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
        )

    def rename(self, key, new_key, collection):
        """
        Renames key to newkey. It returns an error when key does not exist. If newkey
        already exists it is overwritten, when this happens RENAME executes an
        implicit DEL operation, so if the deleted key contains a very big value it
        may cause high latency even if RENAME itself is usually a constant-time
        operation.
        More on https://redis.io/commands/rename/

        :param key: Key to rename
        :type key: str
        :param new_key: New key name
        :type new_key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "RENAME"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, new_key
        )

    def renamenx(self, key, new_key, collection):
        """
        Renames key to newkey if newkey does not yet exist. It returns an error when
        key does not exist. In Cluster mode, both key and newkey must be in the same
        hash slot, meaning that in practice only keys that have the same hash tag can
        be reliably renamed in cluster.
        More on https://redis.io/commands/renamenx/

        :param key: Key to rename
        :type key: str
        :param new_key: New key name
        :type new_key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "RENAMENX"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, key, new_key
        )

    def scan(self, cursor, collection, pattern=None, count=None, data_type=None):
        """
        The SCAN command and the closely related commands SSCAN, HSCAN and ZSCAN are
        used in order to incrementally iterate over a collection of elements.
        More on https://redis.io/commands/scan/

        :param cursor: Cursor value (start with 0)
        :type cursor: int
        :param collection: Name of the collection that we set values to
        :type collection: str
        :param pattern: It is possible to only iterate elements matching a given pattern
        :type pattern: str
        :param count: COUNT the user specified the amount of work that should be done
        :type count: int
        :param data_type: Set TYPE option to only return objects that match a given type
        :type data_type: int
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "SCAN"
        pattern_list = []
        if pattern is not None:
            pattern_list.append("MATCH")
            pattern_list.append(pattern)

        count_list = []
        if count is not None:
            count_list.append("COUNT")
            count_list.append(count)

        type_list = []
        if data_type is not None:
            type_list.append("TYPE")
            type_list.append(data_type)

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, cursor, *pattern_list, *count_list, *type_list
        )

    def ttl(
        self,
        key,
        collection,
    ):
        """
        Returns the remaining time to live of a key that has a timeout. This
        introspection capability allows a Redis client to check how many seconds a
        given key will continue to be part of the dataset.
        More on https://redis.io/commands/ttl/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "TTL"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def type(
        self,
        key,
        collection,
    ):
        """
        Returns the string representation of the type of the value stored at key. The
        different types that can be returned are: string, list, set, zset, hash and
        stream.
        More on https://redis.io/commands/type/

        :param key: Key of the data
        :type key: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "TYPE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            key,
        )

    def unlink(
        self,
        keys,
        collection,
    ):
        """
        This command is very similar to DEL: it removes the specified keys. Just like
        DEL a key is ignored if it does not exist. However the command performs the
        actual memory reclaiming in a different thread, so it is not blocking,
        while DEL is. This is where the command name comes from: the command just
        unlinks the keys from the keyspace. The actual removal will happen later
        asynchronously.
        More on https://redis.io/commands/unlink/

        :param keys: List of keys to be deleted
        :type keys: list
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "UNLINK"
        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, *keys
        )

    def echo(
        self,
        message,
        collection,
    ):
        """
        Returns message.
        More on https://redis.io/commands/echo/

        :param message: Message to be sent
        :type message: str
        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "ECHO"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            message,
        )

    def ping(
        self,
        collection,
        message=None,
    ):
        """
        Returns message.
        More on https://redis.io/commands/ping/

        :param collection: Name of the collection that we set values to
        :type collection: str
        :param message: Message to be sent
        :type message: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "PING"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
            message,
        )

    def dbsize(
        self,
        collection,
    ):
        """
        Return the number of keys in the currently-selected database.
        More on https://redis.io/commands/dbsize/

        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "DBSIZE"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
        )

    def flushdb(self, collection, async_flush=None):
        """
        Delete all the keys of the currently selected DB. This command never fails.
        By default, FLUSHDB will synchronously flush all keys from the database.
        Starting with Redis 6.2, setting the lazyfree-lazy-user-flush configuration
        directive to "yes" changes the default flush mode to asynchronous. It is
        possible to use one of the following modifiers to dictate the flushing mode
        explicitly:
        ASYNC: flushes the database asynchronously
        SYNC: flushes the database synchronously
        More on https://redis.io/commands/flushdb/

        :param collection: Name of the collection that we set values to
        :type collection: str
        :param async_flush: Message to be sent
        :type async_flush: bool
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "FLUSHDB"
        if async_flush is True:
            async_flush_command = "ASYNC"
        else:
            async_flush_command = None

        return RedisInterface(self._conn, self._executor).command_parser(
            command, collection, async_flush_command
        )

    def time(
        self,
        collection,
    ):
        """
        The TIME command returns the current server time as a two items lists: a Unix
        timestamp and the amount of microseconds already elapsed in the current
        second. Basically the interface is very similar to the one of the
        gettimeofday system call.
        More on https://redis.io/commands/time/

        :param collection: Name of the collection that we set values to
        :type collection: str
        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        command = "TIME"
        return RedisInterface(self._conn, self._executor).command_parser(
            command,
            collection,
        )
