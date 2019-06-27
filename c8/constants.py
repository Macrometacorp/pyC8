###############################################################################
#                                                                             #
#  constants.py - Global constants used to connect to Macrometa C8            #
#  @author: Kartikeya IYER (kartikeya@macrometa.co)                           #
#                                                                             #
#  Copyright (c) 2018 Macrometa Corp. All rights reserved.                    #
#                                                                             #
#                                                                             #
#  CHANGELOG:                                                                 #
#      2018-07-23 : Initial cut                                               #
#                                                                             #
#                                                                             #
###############################################################################


# C8DB Defaults
TENANT_DEFAULT = "guest"
FABRIC_DEFAULT = "_system"
USER_DEFAULT = "root"
FABRIC_PORT = "30005"

# Streams defaults
STREAM_PORT = "6650"
STREAM_GLOBAL_NS_PREFIX = "c8global."
STREAM_LOCAL_NS_PREFIX = "c8local."
STREAMNAME_GLOBAL_SYSTEM_DEFAULT = STREAM_GLOBAL_NS_PREFIX + "_system"
STREAMNAME_LOCAL_SYSTEM_DEFAULT = STREAM_LOCAL_NS_PREFIX + "_system"

# TODO : Functions defaults

if __name__ == "__main__":
    print('This file is only meant to be used as an import module')
