#!/usr/bin/python2

# ################################################################################################ #
# AccuRev utility script                                                                           #
# Author: Lazar Sumar                                                                              #
# Date:   06/11/2014                                                                               #
#                                                                                                  #
# This script is a library that is intended to expose a Python API to the AccuRev commands and     #
# command result data structures.                                                                  #
# ################################################################################################ #

import sys
import subprocess
import xml.etree.ElementTree as ElementTree

# ################################################################################################ #
# Script Globals                                                                                   #
# ################################################################################################ #


# ################################################################################################ #
# Script Classes                                                                                   #
# ################################################################################################ #
# The raw class namespaces raw accurev commands that return text output directly from the terminal.
class raw(object):
    # The __lastCommand is used to access the return code that the last command had generated in most
    # cases.
    _lastCommand = None
    
    @staticmethod
    def __RunCommand(cmd, outputFilename=None):
        accurevCommand = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        
        outputFile = None
        if outputFilename is not None:
            outputFile = open(outputFilename, "w")
        
        xmlOutput = ''
        accurevCommand.poll()
        while accurevCommand.returncode is None:
            stdoutdata, stderrdata = accurevCommand.communicate()
            if outputFile is None:
                xmlOutput += stdoutdata
            else:
                outputFile.write(stdoutdata)
            accurevCommand.poll()
        
        raw._lastCommand = accurevCommand
        
        if outputFile is None:
            return xmlOutput
        else:
            return 'Written to ' + outputFilename

    @staticmethod
    def GetAcSync():
        # http://www.accurev.com/download/ac_current_release/AccuRev_WebHelp/AccuRev_Admin/wwhelp/wwhimpl/common/html/wwhelp.htm#href=timewarp.html&single=true
        # The AC_SYNC environment variable controls whether your machine clock being out of sync with
        # the AccuRev server time generates an error or not. Allowed states:
        #   * Not set or set to ERROR   ->   an error occurs and a message appears.
        #   * Set to WARN               ->   a warning is displayed but the command executes.
        #   * Set to IGNORE             ->   no error/warning, command executes.
        return os.environ.get('AC_SYNC')
        
    @staticmethod
    def SetAcSync(value):
        os.environ['AC_SYNC'] = value

    @staticmethod
    def Login(username = None, password = None):
        if username is not None and password is not None:
            accurevCommand = subprocess.Popen([ "accurev", "login" ], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
            accurevCommand.communicate(username + '\n' + password + '\n')
            accurevCommand.wait()
            
            return (accurevCommand.returncode == 0)
        
        return False
        
    @staticmethod
    def Logout():
        accurevCommand = subprocess.Popen([ "accurev", "logout" ])
        accurevCommand.wait()
        
        return (accurevCommand.returncode == 0)

    @staticmethod
    def History(depot=None, stream=None, timeSpec=None, listFile=None, isListFileXml=False, elementList=None, elementId=None, transactionKind=None, username=None, isXmlOutput=False, outputFilename=None):
        cmd = [ "accurev", "hist" ]
        
        # Interpret options
        if depot is not None:
            cmd.extend([ "-p", depot ])
        if stream is not None:
            cmd.extend([ "-s", stream])
        if timeSpec is not None:
            cmd.extend([ "-t", timeSpec])
        if listFile is not None:
            if isListFileXml:
                cmd.append("-Fx")
            cmd.extend([ "-l", listFile])
        if elementList is not None:
            cmd.extend(elementList)
        if elementId is not None:
            cmd.extend([ "-e", elementId])
        if transactionKind is not None:
            cmd.extend([ "-k", transactionKind])
        if username is not None:
            cmd.extend([ "-u", username])
        if isXmlOutput:
            cmd.append("-fx")
        
        return raw.__RunCommand(cmd, outputFilename)

    @staticmethod
    def Populate(isRecursive=False, isOverride=False, verSpec=None, location=None, dontBuildDirTree=False, timeSpec=None, isXmlOutput=False, listFile=None, elementList=None):
        cmd = [ "accurev", "pop" ]
        
        if isOverride:
            cmd.append("-O")
        if isRecursive:
            cmd.append("-R")
        
        if location is not None and verSpec is not None:
            cmd.extend(["-v", verSpec, "-L", location])
            if dontBuildDirTree:
                cmd.append("-D")
        elif location is not None or verSpec is not None:
            raise Exception("""AccuRev populate command must have either both the <ver-spec> and <location>
    supplied or neither. We can infer the <ver-spec> but <location>
    must be specified if it is provided""")
        
        if timeSpec is not None:
            cmd.extend(["-t", timeSpec])
        
        if isXmlOutput:
            cmd.append("-fx")
        
        if listFile is not None:
            cmd.extend(["-l", listFile])
        elif elementList is not None:
            cmd.append(elementList)
        
        return raw.__RunCommand(cmd)
        
# ################################################################################################ #
# Script Functions                                                                                 #
# ################################################################################################ #
def GetAcSync():
    return raw.GetAcSync()
        
def SetAcSync(value):
    raw.SetAcSync(value)

def Login(username = None, password = None):
    return raw.Login(username, password)
    
def Logout():
    return raw.Logout()

def History(depot=None, stream=None, timeSpec=None, listFile=None, isListFileXml=False, elementList=None, elementId=None, transactionKind=None, username=None, isXmlOutput=False, outputFilename=None):
    return raw.History(depot=depot, stream=stream, timeSpec=timeSpec, listFile=listFile, isListFileXml=isListFileXml, elementList=elementList, elementId=elementId, transactionKind=transactionKind, username=username, isXmlOutput=True, outputFilename=outputFilename)

def Populate(isRecursive=False, isOverride=False, verSpec=None, location=None, dontBuildDirTree=False, timeSpec=None, listFile=None, elementList=None):
    output = raw.Populate(isRecursive=isRecursive, isOverride=isOverride, verSpec=verSpec, location=location, dontBuildDirTree=dontBuildDirTree, timeSpec=timeSpec, isXmlOutput=True, listFile=listFile, elementList=elementList)
    
    xmlElement = ElementTree.fromstring(output)
    if xmlElement is not None:
        message = xmlElement.find('message')
        if message is not None:
            errorAttrib = message.attrib.get('error')
            if errorAttrib is not None:
                print "accurev populate error:", message.text
    
    if raw._lastCommand is not None:
        return (raw._lastCommand.returncode == 0)
    return None
        
# ################################################################################################ #
# Script Main                                                                                      #
# ################################################################################################ #
if __name__ == "__main__":
    print "This script is not intended to be run directly..."