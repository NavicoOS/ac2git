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
import datetime
import re

# ################################################################################################ #
# Script Globals                                                                                   #
# ################################################################################################ #


# ################################################################################################ #
# Script Classes                                                                                   #
# ################################################################################################ #
def GetXmlContents(xmlElement):
    if xmlElement is not None:
        return xmlElement.text + ''.join(ElementTree.tostring(e) for e in xmlElement)
    return None

def IntOrNone(value):
    if value is None:
        return None
    return int(value)

def UTCDateTimeOrNone(value):
    if value is None:
        return None
    if type(value) is str or type(value) is int:
        value = float(value)
    return datetime.datetime.utcfromtimestamp(value)

class AccuRevBool(object):
    def __init__(self, value):
        if type(value) is bool:
            self.value = value
            self.originalStr = None
        elif type(value) is str:
            self.value = AccuRevBool.string2bool(value)
            self.originalStr = value
        else:
            raise Exception("Unknown type to convert to AccuRevBool")

    def __nonzero__(self):
        return self.value

    def __bool__(self):
        return self.value

    def __repr__(self):
        if self.originalStr is None:
            return self.toString()
        else:
            return self.originalStr

    def toString(self, toTrueFalse=True, toYesNo=False, toUpper=False, toLower=False):
        rv = None
        if toTrueFalse:
            if self.value:
                rv = "True"
            else:
                rv = "False"
        else: # toYesNo:
            if self.value:
                rv = "Yes"
            else:
                rv = "No"
        if toLower:
            rv = rv.lower()
        elif toUpper:
            rv = rv.upper()
        
        return rv
        
    @staticmethod
    def string2bool(string):
        rv = None
        string = string.lower()

        if string == "yes" or string == "true":
            rv = True
        elif string == "no" or string == "false":
            rv = False
        else:
            raise Exception("AccurevBool value invalid")

        return rv

    @classmethod
    def fromstring(cls, string):
        if string is not None:
            rv = AccuRevBool.string2bool(string)
            return cls(rv)
        return None


class AccuRevWorkspace(object):
    def __init__(self, storage, host, targetTransaction, fileModTime, EOL, Type):
        self.storage           = storage
        self.host              = host
        self.targetTransaction = IntOrNone(targetTransaction)
        self.fileModTime       = UTCDateTimeOrNone(fileModTime)
        self.EOL               = EOL
        self.Type              = Type
        
    def __repr__(self):
        str = "AccuRevWorkspace(storage=" + repr(self.storage)
        str += ", host="               + repr(self.host)
        str += ", targetTransaction="  + repr(self.targetTransaction)
        str += ", fileModTime="        + repr(self.fileModTime)
        str += ", EOL="                + repr(self.EOL)
        str += ", Type="               + repr(self.Type)
        str += ")"
        
        return str
    
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'wspace':
            storage                = xmlElement.attrib.get('Storage')
            host                   = xmlElement.attrib.get('Host')
            targetTransaction      = xmlElement.attrib.get('Target_trans')
            fileModTime            = xmlElement.attrib.get('fileModTime')
            EOL                    = xmlElement.attrib.get('EOL')
            Type                   = xmlElement.attrib.get('Type')
            
            return cls(storage, host, targetTransaction, fileModTime, EOL, Type)
        
        return None
    
class AccuRevStream(object):
    def __init__(self, name, streamNumber, depotName, Type, basis=None, basisStreamNumber=None, time=None, prevTime=None, prevBasis=None, prevBasisStreamNumber=None, workspace=None, startTime=None, isDynamic=None, hasDefaultGroup=None):
        self.name                  = name
        self.streamNumber          = IntOrNone(streamNumber)
        self.depotName             = depotName
        self.Type                  = Type
        self.basis                 = basis
        self.basisStreamNumber     = IntOrNone(basisStreamNumber)
        self.time                  = UTCDateTimeOrNone(time)
        self.prevTime              = UTCDateTimeOrNone(prevTime)
        self.prevBasis             = prevBasis
        self.prevBasisStreamNumber = IntOrNone(prevBasisStreamNumber)
        self.workspace             = workspace
        self.startTime             = UTCDateTimeOrNone(startTime)
        self.isDynamic             = AccuRevBool.fromstring(isDynamic)
        self.hasDefaultGroup       = AccuRevBool.fromstring(hasDefaultGroup)

    def __repr__(self):
        str = "AccuRevStream(name="       + repr(self.name)
        str += ", streamNumber="          + repr(self.streamNumber)
        str += ", depotName="             + repr(self.depotName)
        str += ", Type="                  + repr(self.Type)
        str += ", basis="                 + repr(self.basis)
        str += ", basisStreamNumber="     + repr(self.basisStreamNumber)
        str += ", time="                  + repr(self.time)
        str += ", prevTime="              + repr(self.prevTime)
        str += ", prevBasis="             + repr(self.prevBasis)
        str += ", prevBasisStreamNumber=" + repr(self.prevBasisStreamNumber)
        str += ", workspace="             + repr(self.workspace)
        str += ", startTime="             + repr(self.startTime)
        str += ", isDynamic="             + repr(self.isDynamic)
        str += ", hasDefaultGroup="       + repr(self.hasDefaultGroup)
        str += ")"
        
        return str
    
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'stream':
            name                      = xmlElement.attrib.get('name')
            streamNumber              = xmlElement.attrib.get('streamNumber')
            depotName                 = xmlElement.attrib.get('depotName')
            Type                      = xmlElement.attrib.get('type')
            basis                     = xmlElement.attrib.get('basis')
            basisStreamNumber         = xmlElement.attrib.get('basisStreamNumber')
            time                      = xmlElement.attrib.get('time')
            prevTime                  = xmlElement.attrib.get('prevTime')
            prevBasis                 = xmlElement.attrib.get('prevBasis')
            prevBasisStreamNumber     = xmlElement.attrib.get('prevBasisStreamNumber')
            startTime                 = xmlElement.attrib.get('startTime')
            isDynamic                 = xmlElement.attrib.get('isDynamic')
            hasDefaultGroup           = xmlElement.attrib.get('hasDefaultGroup')
            
            wspaceElement = xmlElement.find('wspace')
            workspace = AccuRevWorkspace.fromxmlelement(wspaceElement)
            
            return cls(name, streamNumber, depotName, Type, basis, basisStreamNumber, time, prevTime, prevBasis, prevBasisStreamNumber, workspace, startTime, isDynamic, hasDefaultGroup)
        
        return None
    
class AccuRevMove(object):
    def __init__(self, dest = None, source = None):
        self.dest   = dest
        self.source = source
        
    def __repr__(self):
        str = "AccuRevMove(dest=" + repr(self.dest)
        str += ", source="        + repr(self.source)
        str += ")"
        
        return str
    
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'move':
            dest                      = xmlElement.attrib.get('dest')
            source                    = xmlElement.attrib.get('source')
            
            return cls(dest, source)
        
        return None
    
class AccuRevVersion(object):
    def __init__(self, stream=None, version=None):
        self.stream  = stream
        self.version = version
    
    def __repr__(self):
        return '{0}/{1}'.format(self.stream, self.version)
        
    @classmethod
    def fromstring(cls, versionString):
        if versionString is not None:
            versionParts = versionString.replace('\\', '/').split('/')
            if len(versionParts) == 2:
                stream  = versionParts[0]
                if re.match('^[0-9]+$', stream):
                    stream = int(stream)
                version = int(versionParts[1])
                
                return cls(stream, version)
        
        return None
    
class AccuRevElementVersion(object):
    def __init__(self, path, eid, virtual, real, virtualNamedVersion, realNamedVersion, ancestor=None, ancestorNamedVersion=None, mergedAgainst=None, mergedAgainstNamedVersion=None, elemType=None, dir=None):
        self.path                      = path
        self.eid                       = IntOrNone(eid)
        self.virtual                   = AccuRevVersion.fromstring(virtual)
        self.real                      = AccuRevVersion.fromstring(real)
        self.virtualNamedVersion       = AccuRevVersion.fromstring(virtualNamedVersion)
        self.realNamedVersion          = AccuRevVersion.fromstring(realNamedVersion)
        self.ancestor                  = AccuRevVersion.fromstring(ancestor)
        self.ancestorNamedVersion      = AccuRevVersion.fromstring(ancestorNamedVersion)
        self.mergedAgainst             = AccuRevVersion.fromstring(mergedAgainst)
        self.mergedAgainstNamedVersion = AccuRevVersion.fromstring(mergedAgainstNamedVersion)
        self.elemType                  = elemType
        self.dir                       = AccuRevBool.fromstring(dir)

    def __repr__(self):
        str = "AccuRevElementVersion(path="    + repr(self.path)
        str += ", eid="                 + repr(self.eid)
        str += ", virtual="             + repr(self.virtual)
        str += ", real="                + repr(self.real)
        str += ", virtualNamedVersion=" + repr(self.virtualNamedVersion)
        str += ", realNamedVersion="    + repr(self.realNamedVersion)
        if self.ancestor is not None or self.ancestorNamedVersion is not None:
            str += ", ancestor="    + repr(self.ancestor)
            str += ", ancestorNamedVersion="    + repr(self.ancestorNamedVersion)
        if self.mergedAgainst is not None or self.mergedAgainstNamedVersion is not None:
            str += ", mergedAgainst="    + repr(self.mergedAgainst)
            str += ", mergedAgainstNamedVersion="    + repr(self.mergedAgainstNamedVersion)
        str += ", elemType="            + repr(self.elemType)
        str += ", dir="                 + repr(self.dir)
        str += ")"
        
        return str
    
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'version':
            path                      = xmlElement.attrib.get('path')
            eid                       = xmlElement.attrib.get('eid')
            virtual                   = xmlElement.attrib.get('virtual')
            real                      = xmlElement.attrib.get('real')
            virtualNamedVersion       = xmlElement.attrib.get('virtualNamedVersion')
            realNamedVersion          = xmlElement.attrib.get('realNamedVersion')
            ancestor                  = xmlElement.attrib.get('ancestor')
            ancestorNamedVersion      = xmlElement.attrib.get('ancestorNamedVersion')
            mergedAgainst             = xmlElement.attrib.get('merged_against')
            mergedAgainstNamedVersion = xmlElement.attrib.get('mergedAgainstNamedVersion')
            elemType                  = xmlElement.attrib.get('elem_type')
            dir                       = xmlElement.attrib.get('dir')
            
            return cls(path, eid, virtual, real, virtualNamedVersion, realNamedVersion, ancestor, ancestorNamedVersion, mergedAgainst, mergedAgainstNamedVersion, elemType, dir)
        
        return None
        
class AccuRevTransaction(object):
    def __init__(self, id, Type, time, user, comment, versions = [], moves = [], stream = None):
        self.id       = IntOrNone(id)
        self.Type     = Type
        self.time     = UTCDateTimeOrNone(time)
        self.user     = user
        self.comment  = comment
        self.versions = versions
        self.moves    = moves
        self.stream   = stream
        
    def __repr__(self):
        str = "AccuRevTransaction(id=" + repr(self.id)
        str += ", Type="               + repr(self.Type)
        str += ", time="               + repr(self.time)
        str += ", user="               + repr(self.user)
        str += ", comment="            + repr(self.comment)
        if len(self.versions) > 0:
            str += ", versions="       + repr(self.versions)
        if len(self.moves) > 0:
            str += ", moves="          + repr(self.moves)
        if self.stream is not None:
            str += ", stream="          + repr(self.stream)
        str += ")"
        
        return str
    
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'transaction':
            id   = xmlElement.attrib.get('id')
            Type = xmlElement.attrib.get('type')
            time = xmlElement.attrib.get('time')
            user = xmlElement.attrib.get('user')
            comment = GetXmlContents(xmlElement.find('comment'))

            versions = []
            for versionElement in xmlElement.findall('version'):
                versions.append(AccuRevElementVersion.fromxmlelement(versionElement))

            moves = []
            for moveElement in xmlElement.findall('move'):
                moves.append(AccuRevMove.fromxmlelement(moveElement))

            streamElement = xmlElement.find('stream')
            stream = AccuRevStream.fromxmlelement(streamElement)

            return cls(id, Type, time, user, comment, versions, moves, stream)

        return None

class AccuRevHistory(object):
    def __init__(self, taskId = None, transactions = [], streams = []):
        self.taskId       = IntOrNone(taskId)
        self.transactions = transactions
        self.streams      = streams

    def __repr__(self):
        str = "AccuRevHistory(taskId=" + repr(self.taskId)
        str += ", transactions="           + repr(self.transactions)
        str += ", streams="                + repr(self.streams)
        str += ")"

        return str

    @classmethod
    def fromxmlstring(cls, xmlText):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlText)
        #xpathPredicate = ".//AcResponse[@Command='hist']"

        if xmlRoot is not None and xmlRoot.tag == "AcResponse" and xmlRoot.get("Command") == "hist":
            # Build the class
            taskId = xmlRoot.attrib.get('TaskId')

            transactions = []
            for transactionElement in xmlRoot.findall('transaction'):
                transactions.append(AccuRevTransaction.fromxmlelement(transactionElement))

            streams = []
            streamsElement = xmlRoot.find('streams')
            if streamsElement is not None:
                for streamElement in streamsElement:
                    streams.append(AccuRevStream.fromxmlelement(streamElement))


            return cls(taskId=taskId, transactions=transactions, streams=streams)
        else:
            # Invalid XML for an AccuRev hist command response.
            return None

class AccuRevStatElement(object):
    def __init__(self, location=None, isDir=False, isExecutable=False, id=None, elemType=None, size=None, modTime=None, hierType=None, virtualVersion=None, namedVersion=None, realVersion=None, status=None):
        self.location       = location
        self.isDir          = AccuRevBool.fromstring(isDir)
        self.isExecutable   = AccuRevBool.fromstring(isExecutable)
        self.id             = IntOrNone(id)
        self.elemType       = elemType
        self.size           = IntOrNone(size)
        self.modTime        = UTCDateTimeOrNone(modTime)
        self.hierType       = hierType
        self.virtualVersion = AccuRevVersion.fromstring(virtualVersion)
        self.namedVersion   = AccuRevVersion.fromstring(namedVersion)
        self.realVersion    = AccuRevVersion.fromstring(realVersion)
        self.status         = status
        self.statusList     = self._ParseStatusIntoList(status)

    def __repr__(self):
        str = "AccuRevStatElement(location=" + repr(self.location)
        str += ", isDir="                    + repr(self.isDir)
        str += ", isExecutable="             + repr(self.isExecutable)
        str += ", id="                       + repr(self.id)
        str += ", elemType="                 + repr(self.elemType)
        str += ", size="                     + repr(self.size)
        str += ", modTime="                  + repr(self.modTime)
        str += ", hierType="                 + repr(self.hierType)
        str += ", virtualVersion="           + repr(self.virtualVersion)
        str += ", namedVersion="             + repr(self.namedVersion)
        str += ", realVersion="              + repr(self.realVersion)
        str += ", status="                   + repr(self.status)
        str += ", statusList="               + repr(self.statusList)
        str += ")"

        return str

    def _ParseStatusIntoList(self, status):
        if status is not None:
            statusList = []
            statusItem = None
            # The following regex takes a parenthesised text like (member)(defunct) and matches it
            # putting the first matched parenthesised expression (of which there could be more than one)
            # into the capture group one.
            # Regex: Match open bracket, consume all characters that are NOT a closed bracket, match the
            #        closed bracket and return the capture group.
            reStatusToken = re.compile("(\\([^\\)]+\\))")
            
            matchObj = reStatusToken.match(status)
            while matchObj and len(status) > 0:
                statusItem = matchObj.group(1)
                statusList.append(statusItem)
                status = status[len(statusItem):]
                matchObj = reStatusToken.match(status)
            
            if len(status) != 0:
                sys.stderr.write("Error: invalidly parsed status! Remaining text is \"{0}\"\n".format(status))
                return None
            return statusList
        return None
        
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == "element":
            # Build the class
            location       = xmlElement.attrib.get('location')
            isDir          = xmlElement.attrib.get('dir')
            isExecutable   = xmlElement.attrib.get('executable')
            id             = xmlElement.attrib.get('id')
            elemType       = xmlElement.attrib.get('elemType')
            size           = xmlElement.attrib.get('size')
            modTime        = xmlElement.attrib.get('modTime')
            hierType       = xmlElement.attrib.get('hierType')
            virtualVersion = xmlElement.attrib.get('Virtual')
            namedVersion   = xmlElement.attrib.get('namedVersion')
            realVersion    = xmlElement.attrib.get('Real')
            status         = xmlElement.attrib.get('status')

            return cls(location=location, isDir=isDir, isExecutable=isExecutable, id=id, elemType=elemType, size=size, modTime=modTime, hierType=hierType, virtualVersion=virtualVersion, namedVersion=namedVersion, realVersion=realVersion, status=status)
        else:
            # Invalid XML for an AccuRev hist command response.
            return None

class AccuRevStat(object):
    def __init__(self, taskId=None, directory=None, elements=[]):
        self.taskId    = IntOrNone(taskId)
        self.directory = directory
        self.elements  = elements

    def __repr__(self):
        str = "AccuRevStat(taskId=" + repr(self.taskId)
        str += ", directory="       + repr(self.directory)
        str += ", elements="        + repr(self.elements)
        str += ")"

        return str

    @classmethod
    def fromxmlstring(cls, xmlText):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlText)
        #xpathPredicate = ".//AcResponse[@Command='hist']"

        if xmlRoot is not None and xmlRoot.tag == "AcResponse" and xmlRoot.get("Command") == "stat":
            # Build the class
            taskId    = xmlRoot.attrib.get('TaskId')
            directory = xmlRoot.attrib.get('Directory')

            elements = []
            for element in xmlRoot.findall('element'):
                elements.append(AccuRevStatElement.fromxmlelement(element))

            return cls(taskId=taskId, directory=directory, elements=elements)
        else:
            # Invalid XML for an AccuRev hist command response.
            return None

class AccuRevUser(object):
    def __init__(self, number = None, name = None, kind = None):
        self.number = IntOrNone(number)
        self.name   = name
        self.kind   = kind
        
    def __repr__(self):
        str = "AccuRevUser(number=" + repr(self.number)
        str += ", name="            + repr(self.name)
        str += ", kind="            + repr(self.kind)
        str += ")"
        
        return str
    
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'Element':
            number = xmlElement.attrib.get('Number')
            name   = xmlElement.attrib.get('Name')
            kind   = xmlElement.attrib.get('Kind')
            
            return cls(number, name, kind)
        
        return None
            
class AccuRevShowUsers(object):
    def __init__(self, taskId = None, users = []):
        self.taskId = IntOrNone(taskId)
        self.users  = users
    
    def __repr__(self):
        str = "AccuRevShowUsers(taskId=" + repr(self.taskId)
        str += ", users="                  + repr(self.users)
        str += ")"
        
        return str
        
    @classmethod
    def fromxmlstring(cls, xmlText):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlText)
        #xpathPredicate = ".//AcResponse[@Command='hist']"
        
        if xmlRoot is not None and xmlRoot.tag == "AcResponse" and xmlRoot.get("Command") == "show users":
            # Build the class
            taskId = xmlRoot.attrib.get('TaskId')
            
            users = []
            for userElement in xmlRoot.findall('Element'):
                users.append(AccuRevUser.fromxmlelement(userElement))
            
            return cls(taskId=taskId, users=users)
        else:
            # Invalid XML for an AccuRev hist command response.
            return None
            
class AccuRevDepot(object):
    def __init__(self, number=None, name=None, slice=None, exclusiveLocking=None, case=None, locWidth=None, replStatus=None):
        self.number           = IntOrNone(number)
        self.name             = name
        self.slice            = slice
        self.exclusiveLocking = exclusiveLocking
        self.case             = case
        self.locWidth         = locWidth
        self.replStatus       = replStatus
        
    def __repr__(self):
        str = "AccuRevDepot(number="  + repr(self.number)
        str += ", name="              + repr(self.name)
        str += ", slice="             + repr(self.slice)
        str += ", exclusiveLocking="  + repr(self.exclusiveLocking)
        str += ", case="              + repr(self.case)
        str += ", locWidth="          + repr(self.locWidth)
        str += ", replStatus="        + repr(self.replStatus)
        str += ")"
        
        return str
    
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'Element':
            number           = xmlElement.attrib.get('Number')
            name             = xmlElement.attrib.get('Name')
            slice            = xmlElement.attrib.get('Slice')
            exclusiveLocking = xmlElement.attrib.get('exclusiveLocking')
            case             = xmlElement.attrib.get('case')
            locWidth         = xmlElement.attrib.get('locWidth')
            replStatus       = xmlElement.attrib.get('ReplStatus')
            
            return cls(number, name, slice, exclusiveLocking, case, locWidth, replStatus)
        
        return None
            
class AccuRevShowDepots(object):
    def __init__(self, taskId = None, depots = []):
        self.taskId = IntOrNone(taskId)
        self.depots = depots
    
    def __repr__(self):
        str = "AccuRevShowDepots(taskId=" + repr(self.taskId)
        str += ", depots="                + repr(self.depots)
        str += ")"
        
        return str
        
    @classmethod
    def fromxmlstring(cls, xmlText):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlText)
        #xpathPredicate = ".//AcResponse[@Command='hist']"
        
        if xmlRoot is not None and xmlRoot.tag == "AcResponse" and xmlRoot.get("Command") == "show depots":
            # Build the class
            taskId = xmlRoot.attrib.get('TaskId')
            
            depots = []
            for depotElement in xmlRoot.findall('Element'):
                depots.append(AccuRevDepot.fromxmlelement(depotElement))
            
            return cls(taskId=taskId, depots=depots)
        else:
            # Invalid XML for an AccuRev hist command response.
            return None
            
class AccuRevShowStreams(object):
    def __init__(self, taskId = None, streams = []):
        self.taskId = IntOrNone(taskId)
        self.streams = streams
    
    def __repr__(self):
        str = "AccuRevShowStreams(taskId=" + repr(self.taskId)
        str += ", streams="                + repr(self.streams)
        str += ")"
        
        return str
        
    @classmethod
    def fromxmlstring(cls, xmlText):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlText)
        #xpathPredicate = ".//AcResponse[@Command='hist']"
        
        if xmlRoot is not None and xmlRoot.tag == "AcResponse" and xmlRoot.get("Command") == "show streams":
            # Build the class
            taskId = xmlRoot.attrib.get('TaskId')
            
            streams = []
            for streamElement in xmlRoot.findall('stream'):
                streams.append(AccuRevStream.fromxmlelement(streamElement))
            
            return cls(taskId=taskId, streams=streams)
        else:
            # Invalid XML for an AccuRev hist command response.
            return None

class AccuRevAncestor(object):
    def __init__(self, location = None, stream = None, version = None, virtualVersion = None):
        self.location       = location
        self.stream         = stream
        self.version        = AccuRevVersion.fromstring(version)
        self.virtualVersion = AccuRevVersion.fromstring(virtualVersion)
    
    def __repr__(self):
        str = "AccuRevUpdate(location=" + repr(self.location)
        str += ", stream="              + repr(self.stream)
        str += ", version="             + repr(self.version)
        str += ", virtualVersion="      + repr(self.virtualVersion)
        str += ")"
        
        return str
        
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'element':
            location       = xmlElement.attrib.get('location')
            stream         = xmlElement.attrib.get('stream')
            version        = xmlElement.attrib.get('version')
            virtualVersion = xmlElement.attrib.get('VirtualVersion')
            
            return cls(location, stream, version, virtualVersion)
            
        return None
    
    @classmethod
    def fromxmlstring(cls, xmlString):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlText)
        #xpathPredicate = ".//AcResponse[@Command='hist']"
        
        if xmlRoot is not None and xmlRoot.tag == "acResponse" and xmlRoot.attrib.get("command") == "anc":
            return AccuRevAncestor.fromxmlelement(xmlRoot.find('element'))
        return None

class AccuRevCommandProgress(object):
    def __init__(self, phase = None, increment = None, number = None):
        self.phase     = phase
        self.increment = increment
        self.number    = IntOrNone(number)
    
    def __repr__(self):
        str = "AccuRevUpdate(phase=" + repr(self.phase)
        str += ", increment="        + repr(self.increment)
        str += ", number="           + repr(self.number)
        str += ")"
        
        return str
        
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'progress':
            phase     = xmlElement.attrib.get('phase')
            increment = xmlElement.attrib.get('increment')
            number    = xmlElement.attrib.get('number')
            
            return cls(phase, increment, number)
            
        return None

class AccuRevUpdateElement(object):
    def __init__(self, location = None):
        self.location = location
    
    def __repr__(self):
        str = "AccuRevUpdate(location=" + repr(self.location)
        str += ")"
        
        return str
        
    @classmethod
    def fromxmlelement(cls, xmlElement):
        if xmlElement is not None and xmlElement.tag == 'element':
            location = xmlElement.attrib.get('location')
            return cls(location)
        return None

class AccuRevUpdate(object):
    def __init__(self, taskId = None, progressItems = None, messages = None, elements = None):
        self.taskId        = IntOrNone(taskId)
        self.progressItems = streams
        self.messages      = messages
        self.elements      = elements
    
    def __repr__(self):
        str = "AccuRevUpdate(taskId=" + repr(self.taskId)
        str += ", progressItems="     + repr(self.progressItems)
        str += ", messages="          + repr(self.messages)
        str += ", elements="          + repr(self.elements)
        str += ")"
        
        return str
        
    @classmethod
    def fromxmlstring(cls, xmlText):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlText)
        #xpathPredicate = ".//AcResponse[@Command='hist']"
        
        if xmlRoot is not None and xmlRoot.tag == "AcResponse" and xmlRoot.get("Command") == "update":
            # Build the class
            taskId = xmlRoot.attrib.get('TaskId')
            
            progressItems = []
            for progressElement in xmlRoot.findall('progress'):
                progressItems.append(AccuRevCommandProgress.fromxmlelement(progressElement))
            
            messages = []
            for messageElement in xmlRoot.findall('message'):
                messages.append(GetXmlContents(messageElement))
            
            elements = []
            for element in xmlRoot.findall('element'):
                messages.append(AccuRevUpdateElement.fromxmlelement(element))
            
            return cls(taskId=taskId, progressItems=progressItems, messages=messages, elements=elements)
        else:
            # Invalid XML for an AccuRev hist command response.
            return None

# The raw class namespaces raw accurev commands that return text output directly from the terminal.
class raw(object):
    # The __lastCommand is used to access the return code that the last command had generated in most
    # cases.
    _lastCommand = None
    _accurevCmd = "accurev"
    
    @staticmethod
    def _runCommand(cmd, outputFilename=None):
        outputFile = None
        if outputFilename is not None:
            outputFile = open(outputFilename, "w")
            accurevCommand = subprocess.Popen(cmd, stdout=outputFile, stdin=subprocess.PIPE)
        else:
            accurevCommand = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
            
        xmlOutput = ''
        accurevCommand.poll()
        while accurevCommand.returncode is None:
            stdoutdata, stderrdata = accurevCommand.communicate()
            if outputFile is None:
                xmlOutput += stdoutdata
            accurevCommand.poll()
        
        raw._lastCommand = accurevCommand
        
        if outputFile is None:
            return xmlOutput
        else:
            outputFile.close()
            return 'Written to ' + outputFilename

    @staticmethod
    def getAcSync():
        # http://www.accurev.com/download/ac_current_release/AccuRev_WebHelp/AccuRev_Admin/wwhelp/wwhimpl/common/html/wwhelp.htm#href=timewarp.html&single=true
        # The AC_SYNC environment variable controls whether your machine clock being out of sync with
        # the AccuRev server time generates an error or not. Allowed states:
        #   * Not set or set to ERROR   ->   an error occurs and a message appears.
        #   * Set to WARN               ->   a warning is displayed but the command executes.
        #   * Set to IGNORE             ->   no error/warning, command executes.
        return os.environ.get('AC_SYNC')
        
    @staticmethod
    def setAcSync(value):
        os.environ['AC_SYNC'] = value

    @staticmethod
    def login(username = None, password = None):
        if username is not None and password is not None:
            accurevCommand = subprocess.Popen([ "accurev", "login" ], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
            accurevCommand.communicate(username + '\n' + password + '\n')
            accurevCommand.wait()
            
            return (accurevCommand.returncode == 0)
        
        return False
        
    @staticmethod
    def logout():
        accurevCommand = subprocess.Popen([ "accurev", "logout" ])
        accurevCommand.wait()
        
        return (accurevCommand.returncode == 0)

    @staticmethod
    def stat(all=False, inBackingStream=False, dispBackingChain=False, defaultGroupOnly=False
            , defunctOnly=False, absolutePaths=False, filesOnly=False, directoriesOnly=False
            , locationsOnly=False, twoLineListing=False, showLinkTarget=False, isXmlOutput=False
            , dispElemID=False, dispElemType=False, strandedElementsOnly=False, keptElementsOnly=False
            , modifiedElementsOnly=False, missingElementsOnly=False, overlapedElementsOnly=False
            , underlapedElementsOnly=False, pendingElementsOnly=False, dontOptimizeSearch=False
            , directoryTreePath=None, stream=None, externalOnly=False, showExcluded=False
            , timeSpec=None, ignorePatternsList=[], listFile=None, elementList=None, outputFilename=None):
        cmd = [ raw._accurevCmd, "stat" ]

        if all:
            cmd.append('-a')
        if inBackingStream:
            cmd.append('-b')
        if dispBackingChain:
            cmd.append('-B')
        if defaultGroupOnly:
            cmd.append('-d')
        if defunctOnly:
            cmd.append('-D')
        
        # Construct the format string
        format = '-f'

        if stream is None:
            # -fa and -fr are not supported when using -s
            if absolutePaths:
                format += 'a'
            else:
                format += 'r'
        if filesOnly:
            format += 'f'
        elif directoriesOnly:
            format += 'd'
        if showLinkTarget:
            format += 'v'
        if isXmlOutput:
            format += 'x'
        if dispElemID:
            format += 'e'
        if dispElemType:
            format += 'k'
        
        if format != '-f':
            cmd.append(format)
        
        # Mutually exclusive parameters.
        if strandedElementsOnly:
            cmd.append('-i')
        elif keptElementsOnly:
            cmd.append('-k')
        elif modifiedElementsOnly:
            cmd.append('-m')
        elif missingElementsOnly:
            cmd.append('-M')
        elif overlapedElementsOnly:
            cmd.append('-o')
        elif pendingElementsOnly:
            cmd.append('-p')
        elif underlapedElementsOnly:
            cmd.append('-U')
        elif externalOnly:
            cmd.append('-x')

        # Remaining parameters
        if dontOptimizeSearch:
            cmd.append('-O')
        if showExcluded:
            cmd.append('-X')
        if directoryTreePath is not None:
            cmd.extend([ '-R', directoryTreePath ])
        if stream is not None:
            cmd.extend([ '-s', str(stream) ])
        if timeSpec is not None:
            cmd.extend([ '-t', str(timeSpec)])
        for ignorePattern in ignorePatternsList:
            cmd.append('--ignore=\"{0}\"'.format(ignorePattern))
        
        if not all and (listFile is None and elementList is None):
            cmd.append('*')
        else:
            if listFile is not None:
                cmd.extend([ '-l', listFile ])
            if elementList is not None:
                if type(elementList) is list:
                    cmd.extend(elementList)
                else:
                    cmd.append(elementList)

        
        return raw._runCommand(cmd, outputFilename)

    # AccuRev history command
    @staticmethod
    def hist(depot=None, stream=None, timeSpec=None, listFile=None, isListFileXml=False, elementList=None, allElementsFlag=False, elementId=None, transactionKind=None, commentString=None, username=None, isXmlOutput=False, outputFilename=None):
        cmd = [ raw._accurevCmd, "hist" ]

        # Interpret options
        if depot is not None:
            cmd.extend([ "-p", depot ])
        if stream is not None:
            cmd.extend([ "-s", str(stream) ])
        if timeSpec is not None:
            if type(timeSpec) is datetime.datetime:
                timeSpecStr = "{:%Y/%m/%d %H:%M:%S}".format(timeSpec)
            else:
                timeSpecStr = str(timeSpec)
            cmd.extend(["-t", str(timeSpecStr)])
        if listFile is not None:
            if isListFileXml:
                cmd.append("-Fx")
            cmd.extend([ "-l", listFile ])
        if elementList is not None:
            if type(elementList) is list:
                cmd.extend(elementList)
            else:
                cmd.append(elementList)
        if allElementsFlag:
            cmd.append("-a")
        if elementId is not None:
            cmd.extend([ "-e", str(elementId) ])
        if transactionKind is not None:
            cmd.extend([ "-k", transactionKind ])
        if commentString is not None:
            cmd.extend([ "-c", commentString ])
        if username is not None:
            cmd.extend([ "-u", username ])
        if isXmlOutput:
            cmd.append("-fx")
        
        return raw._runCommand(cmd, outputFilename)

    # AccuRev populate command
    @staticmethod
    def pop(isRecursive=False, isOverride=False, verSpec=None, location=None, dontBuildDirTree=False, timeSpec=None, isXmlOutput=False, listFile=None, elementList=None):
        cmd = [ raw._accurevCmd, "pop" ]
        
        if isOverride:
            cmd.append("-O")
        if isRecursive:
            cmd.append("-R")
        
        if location is not None and verSpec is not None:
            cmd.extend(["-v", str(verSpec), "-L", location])
            if dontBuildDirTree:
                cmd.append("-D")
        elif location is not None or verSpec is not None:
            raise Exception("""AccuRev populate command must have either both the <ver-spec> and <location>
    supplied or neither. We can infer the <ver-spec> but <location>
    must be specified if it is provided""")
        
        if timeSpec is not None:
            if type(timeSpec) is datetime.datetime:
                timeSpecStr = "{:%Y/%m/%d %H:%M:%S}".format(timeSpec)
            else:
                timeSpecStr = str(timeSpec)
            cmd.extend(["-t", str(timeSpecStr)])
        
        if isXmlOutput:
            cmd.append("-fx")
        
        if listFile is not None:
            cmd.extend(["-l", listFile])
        if elementList is not None:
            if type(elementList) is list:
                cmd.extend(elementList)
            else:
                cmd.append(elementList)
        
        return raw._runCommand(cmd)

    # AccuRev checkout command
    @staticmethod
    def co(comment=None, selectAllModified=False, verSpec=None, isRecursive=False, transactionNumber=None, elementId=None, listFile=None, elementList=None):
        cmd = [ raw._accurevCmd, "co" ]
        
        if comment is not None:
            cmd.extend([ '-c', comment ])
        if selectAllModified:
            cmd.append('-n')
        if verSpec is not None:
            cmd.extend([ '-v', str(verSpec) ])
        if isRecursive:
            cmd.append('-R')
        if transactionNumber is not None:
            cmd.extend([ '-t', transactionNumber ])
        if elementId is not None:
            cmd.extend([ '-e', str(elementId) ])
        if listFile is not None:
            cmd.extend([ '-l', listFile ])
        if elementList is not None:
            if type(elementList) is list:
                cmd.extend(elementList)
            else:
                cmd.append(elementList)
        
        return raw._runCommand(cmd)
        
    @staticmethod
    def cat(elementId=None, element=None, depotName=None, verSpec=None, outputFilename=None):
        cmd = [ raw._accurevCmd, "cat" ]
        
        if verSpec is not None:
            cmd.extend([ '-v', str(verSpec) ])
        if depotName is not None:
            cmd.extend([ '-p', depotName ])
        
        if elementId is not None:
            cmd.extend([ '-e', str(elementId) ])
        elif element is not None:
            cmd.append(element)
        else:
            raise Exception('accurev cat command needs either an <element> or an <eid> to be specified')
            
        return raw._runCommand(cmd, outputFilename)
        
    @staticmethod
    def purge(comment=None, stream=None, issueNumber=None, elementList=None, listFile=None, elementId=None):
        cmd = [ raw._accurevCmd, "purge" ]
        
        if comment is not None:
            cmd.extend([ '-c', comment ])
        if stream is not None:
            cmd.extend([ '-s', str(stream) ])
        if issueNumber is not None:
            cmd.extend([ '-I', issueNumber ])
        if elementList is not None:
            if type(elementList) is list:
                cmd.extend(elementList)
            else:
                cmd.append(elementList)
        if listFile is not None:
            cmd.extend([ '-l', listFile ])
        if elementId is not None:
            cmd.extend([ '-e', str(elementId) ])
        
        return raw._runCommand(cmd)
    
    # AccuRev ancestor command
    @staticmethod
    def anc(element, commonAncestor=False, versionId=None, basisVersion=False, commonAncestorOrBasis=False, prevVersion=False, isXmlOutput=False):
        # The anc command determines one of the following:
        #  * the direct ancestor (predecessor) version of a particular version
        #  * the version that preceded a particular version in a specified stream
        #  * the basis version corresponding to a particular version
        #  * the common ancestor of two versions
        # In its simplest form (no command-line options), anc reports the direct ancestor of the version in
        # your workspace for the specified element.
        cmd = [ raw._accurevCmd, "anc" ]
        
        if commonAncestor:
            cmd.append('-c')
        if versionId is not None:
            cmd.extend([ '-v', versionId ])
        if basisVersion:
            cmd.append('-j')
        if commonAncestorOrBasis:
            cmd.append('-J')
        if prevVersion:
            cmd.append('-1')
        if isXmlOutput:
            cmd.append('-fx')
        
        cmd.append(element)
        
        return raw._runCommand(cmd)
    
    @staticmethod
    def chstream(stream, newBackingStream=None, timeSpec=None, newName=None):
        cmd = [ raw._accurevCmd, "chstream", "-s", str(stream) ]
        
        if newName is not None and (newBackingStream is not None or timeSpec is not None):
            raise Exception('accurev.raw.Chstream does not accept the newName parameter if any other parameter is passed!')
        else:
            if newBackingStream is not None:
                cmd.extend([ '-b', newBackingStream ])
            if timeSpec is not None:
                if type(timeSpec) is datetime.datetime:
                    timeSpecStr = "{:%Y/%m/%d %H:%M:%S}".format(timeSpec)
                else:
                    timeSpecStr = str(timeSpec)
                cmd.extend(["-t", str(timeSpecStr)])
            if newName is not None:
                renameCmd.append(newName)
            
            return raw._runCommand(cmd)
        
    @staticmethod
    def chws(workspace, newBackingStream=None, newLocation=None, newMachine=None, kind=None, eolType=None, isMyWorkspace=True, newName=None):
        cmd = [ raw._accurevCmd, "chws" ]
        
        if isMyWorkspace:
            cmd.extend([ '-w', workspace ])
        else:
            cmd.extend([ '-s', workspace ])
        
        if newBackingStream is not None:
            cmd.extend([ '-b', newBackingStream ])
        if newLocation is not None:
            cmd.extend([ '-l', newLocation ])
        if newMachine is not None:
            cmd.extend([ '-m', newMachine ])
        if kind is not None:
            cmd.extend([ '-k', kind ])
        if eolType is not None:
            cmd.extend([ '-e', eolType ])
        if newName is not None:
            renameCmd.append(newName)
        
        return raw._runCommand(cmd)
        
    @staticmethod
    def update(refTree=None, doPreview=False, transactionNumber=None, mergeOnUpdate=False, isXmlOutput=False, isOverride=False, outputFilename=None):
        cmd = [ raw._accurevCmd, "update" ]
        
        if refTree is not None:
            cmd.extend([ '-r', refTree ])
        if doPreview:
            cmd.append('-i')
        if transactionNumber is not None:
            cmd.extend([ '-t', transactionNumber ])
        if mergeOnUpdate:
            cmd.append('-m')
        if isXmlOutput:
            cmd.append('-fx')
        if isOverride:
            cmd.append('-O')
        
        return raw._runCommand(cmd, outputFilename)
        
    class show(object):
        @staticmethod
        def _getShowBaseCommand(isXmlOutput=False, includeDeactivatedItems=False, includeOldDefinitions=False, addKindColumnForUsers=False, includeHasDefaultGroupAttribute=False):
            # See: http://www.accurev.com/download/ac_current_release/AccuRev_WebHelp/wwhelp/wwhimpl/js/html/wwhelp.htm#href=AccuRev_User_CLI/cli_ref_show.html
            # for usage.
            cmd = [ raw._accurevCmd, "show" ]
            
            flags = ''
            
            if includeDeactivatedItems and includeOldDefinitions:
                flags += 'I'
            elif includeDeactivatedItems:
                flags += 'i'
            
            if addKindColumnForUsers:
                flags += 'v'
            
            if includeHasDefaultGroupAttribute:
                # This option forces the XML output.
                flags += 'xg'
            elif isXmlOutput:
                flags += 'x'
            
            if len(flags) > 0:
                flags = '-f' + flags
                cmd.append('-f{0}'.format(flags))
            
            return cmd
    
        @staticmethod
        def _runSimpleShowSubcommand(subcommand, isXmlOutput=False, includeDeactivatedItems=False):
            if subcommand is not None:
                cmd = raw.show._getShowBaseCommand(isXmlOutput=isXmlOutput, includeDeactivatedItems=includeDeactivatedItems)
                cmd.append(subcommand)
                return raw._runCommand(cmd)
                
            return None
    
        @staticmethod
        def users(isXmlOutput=False, includeDeactivatedItems=False, addKindColumnForUsers=False):
            return raw.Show._runSimpleShowSubcommand(subcommand="users", isXmlOutput=isXmlOutput, includeDeactivatedItems=includeDeactivatedItems, addKindColumnForUsers=addKindColumnForUsers)
        
        @staticmethod
        def depots(isXmlOutput=False, includeDeactivatedItems=False):
            return raw.show._runSimpleShowSubcommand(subcommand="depots", isXmlOutput=isXmlOutput, includeDeactivatedItems=includeDeactivatedItems)

        @staticmethod
        def streams(depot=None, timeSpec=None, stream=None, matchType=None, listFile=None, listPathAndChildren=False, listChildren=False, listImmediateChildren=False, nonEmptyDefaultGroupsOnly=False, isXmlOutput=False, includeDeactivatedItems=False, includeOldDefinitions=False, includeHasDefaultGroupAttribute=False):
            cmd = raw.show._getShowBaseCommand(isXmlOutput=isXmlOutput, includeDeactivatedItems=includeDeactivatedItems, includeOldDefinitions=includeOldDefinitions, includeHasDefaultGroupAttribute=includeHasDefaultGroupAttribute)

            if depot is not None:
                cmd.extend([ "-p", depot ])
            if timeSpec is not None:
                if type(timeSpec) is datetime.datetime:
                    timeSpecStr = "{:%Y/%m/%d %H:%M:%S}".format(timeSpec)
                else:
                    timeSpecStr = str(timeSpec)
                cmd.extend(["-t", str(timeSpecStr)])
            if stream is not None:
                cmd.extend([ "-s", str(stream) ])
            if matchType is not None:
                cmd.extend([ "-m", matchType ])
            if listFile is not None:
                cmd.extend([ "-l", listFile ])
            
            if listPathAndChildren:
                cmd.append("-r")
            elif listChildren:
                cmd.append("-R")
            elif listImmediateChildren:
                cmd.append("-1")
                
            cmd.append("streams")
            
            return raw._runCommand(cmd)
    
    class replica(object):
        @staticmethod
        def sync():
            cmd = [ raw._accurevCmd, "replica", "sync" ]
            
            return raw._runCommand(cmd)
    
# ################################################################################################ #
# Script Functions                                                                                 #
# ################################################################################################ #
def getAcSync():
    return raw.getAcSync()
        
def setAcSync(value):
    raw.setAcSync(value)

def login(username = None, password = None):
    return raw.login(username, password)
    
def logout():
    return raw.logout()

def stat(all=False, inBackingStream=False, dispBackingChain=False, defaultGroupOnly=False
        , defunctOnly=False, absolutePaths=False, filesOnly=False, directoriesOnly=False
        , locationsOnly=False, twoLineListing=False, showLinkTarget=False
        , dispElemID=False, dispElemType=False, strandedElementsOnly=False, keptElementsOnly=False
        , modifiedElementsOnly=False, missingElementsOnly=False, overlapedElementsOnly=False
        , underlapedElementsOnly=False, pendingElementsOnly=False, dontOptimizeSearch=False
        , directoryTreePath=None, stream=None, externalOnly=False, showExcluded=False
        , timeSpec=None, ignorePatternsList=[], listFile=None, elementList=None, outputFilename=None):
    outputXml = raw.stat(all=all, inBackingStream=inBackingStream, dispBackingChain=dispBackingChain, defaultGroupOnly=defaultGroupOnly
        , defunctOnly=defunctOnly, absolutePaths=absolutePaths, filesOnly=filesOnly, directoriesOnly=directoriesOnly
        , locationsOnly=locationsOnly, twoLineListing=twoLineListing, showLinkTarget=showLinkTarget, isXmlOutput=True
        , dispElemID=dispElemID, dispElemType=dispElemType, strandedElementsOnly=strandedElementsOnly, keptElementsOnly=keptElementsOnly
        , modifiedElementsOnly=modifiedElementsOnly, missingElementsOnly=missingElementsOnly, overlapedElementsOnly=overlapedElementsOnly
        , underlapedElementsOnly=underlapedElementsOnly, pendingElementsOnly=pendingElementsOnly, dontOptimizeSearch=dontOptimizeSearch
        , directoryTreePath=directoryTreePath, stream=stream, externalOnly=externalOnly, showExcluded=showExcluded
        , timeSpec=timeSpec, ignorePatternsList=ignorePatternsList, listFile=listFile, elementList=elementList, outputFilename=outputFilename)
    if raw._lastCommand.returncode == 0:
        return AccuRevStat.fromxmlstring(outputXml)
    else:
        return None

# AccuRev history command
def hist(depot=None, stream=None, timeSpec=None, listFile=None, isListFileXml=False, elementList=None, allElementsFlag=False, elementId=None, transactionKind=None, commentString=None, username=None, outputFilename=None):
    xmlOutput = raw.hist(depot=depot, stream=stream, timeSpec=timeSpec, listFile=listFile, isListFileXml=isListFileXml, elementList=elementList, allElementsFlag=allElementsFlag, elementId=elementId, transactionKind=transactionKind, commentString=commentString, username=username, isXmlOutput=True, outputFilename=outputFilename)
    return AccuRevHistory.fromxmlstring(xmlOutput)

# AccuRev Populate command
def pop(isRecursive=False, isOverride=False, verSpec=None, location=None, dontBuildDirTree=False, timeSpec=None, listFile=None, elementList=None):
    output = raw.pop(isRecursive=isRecursive, isOverride=isOverride, verSpec=verSpec, location=location, dontBuildDirTree=dontBuildDirTree, timeSpec=timeSpec, isXmlOutput=True, listFile=listFile, elementList=elementList)

    xmlElement = ElementTree.fromstring(output)
    if xmlElement is not None:
        message = xmlElement.find('message')
        if message is not None:
            errorAttrib = message.attrib.get('error')
            if errorAttrib is not None:
                sys.stderr.write("accurev populate error:\n{0}\n".format(message.text))
    
    if raw._lastCommand is not None:
        return (raw._lastCommand.returncode == 0)
    return None

# AccuRev checkout command
def co(comment=None, selectAllModified=False, verSpec=None, isRecursive=False, transactionNumber=None, elementId=None, listFile=None, elementList=None):
    output = raw.oo(comment=comment, selectAllModified=selectAllModified, verSpec=verSpec, isRecursive=isRecursive, transactionNumber=transactionNumber, elementId=elementId, listFile=listFile, elementList=elementList)
    if raw._lastCommand is not None:
        return (raw._lastCommand.returncode == 0)
    return None

def cat(elementId=None, element=None, depotName=None, verSpec=None, outputFilename=None):
    output = raw.cat(elementId=elementId, element=element, depotName=depotName, verSpec=verSpec, outputFilename=outputFilename)
    if raw._lastCommand is not None:
        return output
    return None

def purge(comment=None, stream=None, issueNumber=None, elementList=None, listFile=None, elementId=None):
    output = raw.purge(comment=comment, stream=stream, issueNumber=issueNumber, elementList=elementList, listFile=listFile, elementId=elementId)
    if raw._lastCommand is not None:
        return (raw._lastCommand.returncode == 0)
    return None

# AccuRev ancestor command
def anc(element, commonAncestor=False, versionId=None, basisVersion=False, commonAncestorOrBasis=False, prevVersion=False):
    outputXml = raw.anc(element, commonAncestor=False, versionId=None, basisVersion=False, commonAncestorOrBasis=False, prevVersion=False, isXmlOutput=True)
    return AccuRevAncestor.fromxmlstring(outputXml)
    
def chstream(stream, newBackingStream=None, timeSpec=None, newName=None):
    raw.chstream(stream=stream, newBackingStream=newBackingStream, timeSpec=timeSpec, newName=newName)
    if raw._lastCommand is not None:
        return (raw._lastCommand.returncode == 0)
    return None
    
def chws(workspace, newBackingStream=None, newLocation=None, newMachine=None, kind=None, eolType=None, isMyWorkspace=True, newName=None):
    raw.chws(workspace=workspace, newBackingStream=newBackingStream, newLocation=newLocation, newMachine=newMachine, kind=kind, eolType=eolType, isMyWorkspace=isMyWorkspace, newName=newName)
    if raw._lastCommand is not None:
        return (raw._lastCommand.returncode == 0)
    return None
        
def update(refTree=None, doPreview=False, transactionNumber=None, mergeOnUpdate=False, isOverride=False, outputFilename=None):
    outputXml = raw.update(refTree=refTree, doPreview=doPreview, transactionNumber=transactionNumber, mergeOnUpdate=mergeOnUpdate, isXmlOutput=True, isOverride=isOverride, outputFilename=outputFilename)
    return AccuRevUpdate.fromxmlstring(outputXml)
    
class show(object):
    @staticmethod
    def users():
        xmlOutput = raw.show.users(isXmlOutput=True)
        return AccuRevShowUsers.fromxmlstring(xmlOutput)
    
    @staticmethod
    def depots():
        xmlOutput = raw.show.depots(isXmlOutput=True)
        return AccuRevShowDepots.fromxmlstring(xmlOutput)

    @staticmethod
    def streams(depot=None, timeSpec=None, stream=None, matchType=None, listFile=None, listPathAndChildren=False, listChildren=False, listImmediateChildren=False, nonEmptyDefaultGroupsOnly=False, includeDeactivatedItems=False, includeOldDefinitions=False):
        xmlOutput = raw.show.streams(depot=depot, timeSpec=timeSpec, stream=stream, matchType=matchType, listFile=listFile, listPathAndChildren=listPathAndChildren, listChildren=listChildren, listImmediateChildren=listImmediateChildren, nonEmptyDefaultGroupsOnly=nonEmptyDefaultGroupsOnly, isXmlOutput=True, includeDeactivatedItems=includeDeactivatedItems, includeOldDefinitions=includeOldDefinitions, includeHasDefaultGroupAttribute=True)
        return AccuRevShowStreams.fromxmlstring(xmlOutput)

class replica(object):
    @staticmethod
    def sync():
        raw.replica.sync()
        if raw._lastCommand is not None:
            return (raw._lastCommand.returncode == 0)
        return None
        
# ################################################################################################ #
# Script Main                                                                                      #
# ################################################################################################ #
if __name__ == "__main__":
    print "This script is not intended to be run directly..."