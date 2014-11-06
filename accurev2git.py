#!/usr/bin/python2

# ################################################################################################ #
# AccuRev to Git conversion script                                                                 #
# Author: Lazar Sumar                                                                              #
# Date:   06/11/2014                                                                               #
#                                                                                                  #
# This script is intended to convert an entire AccuRev depot into a git repository converting      #
# workspaces and streams into branches and respecting merges.                                      #
# ################################################################################################ #

import sys
import accurev

# ################################################################################################ #
# Script Classes                                                                                   #
# ################################################################################################ #


# ################################################################################################ #
# Script Main                                                                                      #
# ################################################################################################ #
def AccuRev2GitMain(argv):
    accurev.Login("", "")
    
    xmlOutput = accurev.History(depot="depot", timeSpec="197177-now.3", isXmlOutput=True)
    print xmlOutput
    
    rv = accurev.Populate(verSpec="stream/1", location="here/we/go")
    print "Populate:", rv
    
    print "Logout:", accurev.Logout()

# ################################################################################################ #
# Script Start                                                                                     #
# ################################################################################################ #
if __name__ == "__main__":
    AccuRev2GitMain(sys.argv)
