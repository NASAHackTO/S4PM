#!/usr/bin/perl

=head1 NAME

s4pm_bootstrap_dprep.pl - generate DN to bootstrap DPREP algorithms

=head1 SYNOPSIS

s4pm_bootstrap_dprep.pl

=head1 DESCRIPTION

This script is used when there are no available look-behind files to kick off 
the DPREP algorthims. The first failed Ephemeris algorithm should be terminated 
and then this algorithm should be invoked. A GUI will appear to enter the 
required orbit number.

=head1 AUTHOR

William E. Smith, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_bootstrap_dprep.pl,v 1.4 2008/02/21 19:48:56 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

# Initialize
#

use Tk;
use strict;
use Cwd;
use S4P;
use File::Basename;

my $orbit = "";
my $passi = "";
my $retc="";

my $mw = MainWindow->new;
$mw->title("    DPREP Bootstrap    ");
$mw->Label(-text => "Enter Orbit Number")->pack(-expand => 1);
$mw->Entry(-textvariable => \$orbit)->pack(-expand => 1);

$mw->Button(-text => "Submit", -command => sub { 
    $retc = main_proc($orbit); 
    if($retc == 1){
        my $ew = MainWindow->new;
        $ew->title("ERROR");
        $ew->Label(-text => "Incorrect Password!")->pack(-expand => 1);
        $ew->Button(-text => "Try Again", -command => sub{$ew->destroy();} )->pack(-expand => 1);
    }
    if($retc == 2){
        my $ew = MainWindow->new;
        $ew->title("ERROR");
        $ew->Label(-text => "Invalid Orbit Number!")->pack(-expand => 1);
        $ew->Button(-text => "Try Again", -command => sub{$ew->destroy();} )->pack(-expand => 1);
    }
})->pack(-expand => 1);
MainLoop;

sub main_proc{

    my $orbit = shift;

# Orbit number sanity check.

# If orbit number was blank, return.
# If orbit number is less than or equal to zero, return.

if( $orbit eq "" or  $orbit <= 0 ) { return 2; }

my $dum      = "";
my $dumsize  = "";
my $ESDT     = "";
my $metasize = "";
my $PGEI     = "";
my $VERSION  = "";

my @GDIR = ();

use vars qw(
    $PGE
    $PROC
    $WORKDIR
    $platform
    $startdate
    $starttime
    $enddate
    $endtime
);

$WORKDIR = cwd;
$PROC = "forward";

@GDIR = split("\/",$WORKDIR);
$platform = $GDIR[-1];

if($platform =~ /AM1Eph/){
    $PGE  = "AM1Eph";
    $PGEI = "AM1EphInit";
    $ESDT = "AM1EPHI";
}elsif($platform =~ /PM1DefEph/){
    $PGE  = "PM1DefEph";
    $PGEI = "PM1DefEphI";
    $ESDT = "PM1EPHDI";
}elsif($platform =~ /AuraEph/){
    $PGE  = "AuraEph";
    $PGEI = "AuraEphI";
    $ESDT = "AUREPHMFI";
}else{
    S4P::perish(30, "main: Invalid working directory: [$platform]");
}

#Get PGE parameters from the specify data config file

my $specify_config = "../../prepare_run/s4pm_select_data_$PGEI.cfg" ;

open(SDCF,"<$specify_config") or 
    S4P::perish(30, "main: Could not open specify data config file: [$specify_config]");

while(<SDCF>){
    chomp;
    if(/algorithm_version/){
        ($dum,$dum,$VERSION)=split(" ");
        $dum="";
        ($dum,$VERSION,$dum)=split("'",$VERSION);
    }
}
close SDCF;

#
# Get date and time of first granule
#
&getdatetime;

my $basename = "$PGEI#$startdate#$starttime";
my $dnfile = "S4PM.DN.0117200118180404";
#
# Create dummy data file
#

my $dumfile = "../../DATA/INPUT/$basename";
open(DUM,">$dumfile") or 
    S4P::perish(110, "main: Could not open dummy file: [$dumfile]");
print DUM <<EOF;
This is a dummy initialization file for AM1EphInit.
EOF
close DUM;

#
# Create dummy metadata file
#

my $metfile = "../../DATA/INPUT/$basename.met";
open(MET,">$metfile") or 
    S4P::perish(111, "main: Could not open metadata file: [$metfile]");

print MET <<EOF;
GROUP = INVENTORYMETADATA
GROUPTYPE = MASTERGROUP
    GROUP = CollectionDescriptionClass
        OBJECT = ShortName
            Value = "$ESDT"
            TYPE = "STRING"
            NUM_VAL = 1
        END_OBJECT = ShortName
        OBJECT = VersionID
            Value = 1
            TYPE = "INTEGER"
            NUM_VAL = 1
        END_OBJECT = VersionID
    END_GROUP = CollectionDescriptionClass
    GROUP = ECSDataGranule
        OBJECT = SizeMBECSDataGranule
            Value = 0.4501119852066040
            TYPE = "DOUBLE"
            NUM_VAL = 1
        END_OBJECT = SizeMBECSDataGranule
        OBJECT = LocalGranuleID
            Value = "$basename"
            TYPE = "STRING"
            NUM_VAL = 1
        END_OBJECT = LocalGranuleID
        OBJECT = ProductionDateTime
            Value = "2001-07-05T22:37:44Z"
            TYPE = "TIME"
            NUM_VAL = 1
        END_OBJECT = ProductionDateTime
    END_GROUP = ECSDataGranule
    GROUP = PGEVersionClass
    END_GROUP = PGEVersionClass
    GROUP = RangeDateTime
        OBJECT = RangeEndingTime
            Value = "$endtime"
            TYPE = "STRING"
            NUM_VAL = 1
        END_OBJECT = RangeEndingTime
        OBJECT = RangeEndingDate
            Value = "$enddate"
            TYPE = "DATE"
            NUM_VAL = 1
        END_OBJECT = RangeEndingDate
        OBJECT = RangeBeginningTime
            Value = "$starttime"
            TYPE = "STRING"
            NUM_VAL = 1
        END_OBJECT = RangeBeginningTime
        OBJECT = RangeBeginningDate
            Value = "$startdate"
            TYPE = "DATE"
            NUM_VAL = 1
        END_OBJECT = RangeBeginningDate
    END_GROUP = RangeDateTime
    GROUP = SpatialDomainContainer
        GROUP = HorizontalSpatialDomainContainer
            GROUP = ZoneIdentifierClass
            END_GROUP = ZoneIdentifierClass
        END_GROUP = HorizontalSpatialDomainContainer
    END_GROUP = SpatialDomainContainer
    GROUP = AdditionalAttributes
        OBJECT = AdditionalAttributesContainer
            CLASS = "1"
            OBJECT = AdditionalAttributeName
                CLASS = "1"
                Value = "FDDEndOrbitNumber"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = AdditionalAttributeName
            GROUP = InformationContent
                CLASS = "1"
                OBJECT = ParameterValue
                    CLASS = "1"
                    Value = ("7244")
                    TYPE = "STRING"
                    NUM_VAL = 1
                END_OBJECT = ParameterValue
            END_GROUP = InformationContent
        END_OBJECT = AdditionalAttributesContainer
        OBJECT = AdditionalAttributesContainer
            CLASS = "2"
            OBJECT = AdditionalAttributeName
                CLASS = "2"
                Value = "FDDStartOrbitNumber"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = AdditionalAttributeName
            GROUP = InformationContent
                CLASS = "2"
                OBJECT = ParameterValue
                    CLASS = "2"
                    Value = ("7243")
                    TYPE = "STRING"
                    NUM_VAL = 1
                END_OBJECT = ParameterValue
            END_GROUP = InformationContent
        END_OBJECT = AdditionalAttributesContainer
    END_GROUP = AdditionalAttributes
END_GROUP = INVENTORYMETADATA
GROUP = COLLECTIONMETADATA
GROUPTYPE = MASTERGROUP
   OBJECT = DLLName
      Mandatory = "TRUE"
      Data_Location = "MCF"
      NUM_VAL = 1
      TYPE = "STRING"
      Value = "libDsESDTSyBASIC.001Sh.so"
   END_OBJECT = DLLName
   OBJECT = spatialSearchType
      Mandatory = "TRUE"
      Data_Location = "MCF"
      NUM_VAL = 1
      TYPE = "STRING"
      Value = "NotSupported"
   END_OBJECT = spatialSearchType
   GROUP = CollectionDescriptionClass
      OBJECT = VersionID
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "INTEGER"
         Value = 1
      END_OBJECT = VersionID
      OBJECT = LongName
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "FDD Definitive Attitude Data for EOS AM-1"
      END_OBJECT = LongName
      OBJECT = ShortName
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "$ESDT"
      END_OBJECT = ShortName
      OBJECT = CollectionDescription
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "Definitive Attitude Data for EOS AM-1 ingested from Flight Dynamics Facility (FDD)"
      END_OBJECT = CollectionDescription
   END_GROUP = CollectionDescriptionClass
   GROUP = ECSCollection
      OBJECT = VersionDescription
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "Initial Drop4 Descriptor"
      END_OBJECT = VersionDescription
      OBJECT = ProcessingCenter
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "GSFC"
      END_OBJECT = ProcessingCenter
      OBJECT = ArchiveCenter
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "GSFC"
      END_OBJECT = ArchiveCenter
      OBJECT = SuggestedUsage
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "None"
      END_OBJECT = SuggestedUsage
      OBJECT = RevisionDate
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "DATE"
         Value = 2001-07-11
      END_OBJECT = RevisionDate
   END_GROUP = ECSCollection
   GROUP = SingleTypeCollection
      OBJECT = AccessConstraints
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "None"
      END_OBJECT = AccessConstraints
      OBJECT = MaintenanceandUpdateFrequency
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "Continually"
      END_OBJECT = MaintenanceandUpdateFrequency
      OBJECT = CollectionState
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "In Work"
      END_OBJECT = CollectionState
   END_GROUP = SingleTypeCollection
   GROUP = Temporal
      GROUP = RangeDateTime
         OBJECT = RangeEndingDate
            Mandatory = "TRUE"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "DATE"
            Value = 1998-06-30
         END_OBJECT = RangeEndingDate
         OBJECT = RangeBeginningDate
            Mandatory = "TRUE"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "DATE"
            Value = 1998-06-30
         END_OBJECT = RangeBeginningDate
         OBJECT = RangeBeginningTime
            Mandatory = "TRUE"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "TIME"
            Value = "00:00:00.00000"
         END_OBJECT = RangeBeginningTime
         OBJECT = RangeEndingTime
            Mandatory = "TRUE"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "TIME"
            Value = "00:00:00.00000"
         END_OBJECT = RangeEndingTime
      END_GROUP = RangeDateTime
      OBJECT = DateType
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "Gregorian"
      END_OBJECT = DateType
      OBJECT = TimeType
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "UTC"
      END_OBJECT = TimeType
      OBJECT = TemporalRangeType
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "Continuous Range"
      END_OBJECT = TemporalRangeType
      OBJECT = EndsatPresentFlag
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "Y"
      END_OBJECT = EndsatPresentFlag
      OBJECT = PrecisionofSeconds
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "INTEGER"
         Value = 3
      END_OBJECT = PrecisionofSeconds
   END_GROUP = Temporal
   GROUP = ProcessingLevel
      OBJECT = ProcessingLevelDescription
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "Telemetry Data"
      END_OBJECT = ProcessingLevelDescription
      OBJECT = ProcessingLevelID
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "0"
      END_OBJECT = ProcessingLevelID
   END_GROUP = ProcessingLevel
   GROUP = Contact
      GROUP = ContactOrganization
         OBJECT = ContactOrganizationContainer
            Data_Location = "NONE"
            Mandatory = "TRUE"
            CLASS = "1"
            GROUP = OrganizationEmail
               CLASS = "1"
               OBJECT = ElectronicMailAddress
                  Mandatory = "TRUE"
                  Data_Location = "MCF"
                  NUM_VAL = 1
                  TYPE = "STRING"
                  Value = ("help-disc\@listserv.gsfc.nasa.gov")
               END_OBJECT = ElectronicMailAddress
            END_GROUP = OrganizationEmail
            GROUP = OrganizationTelephone
               CLASS = "1"
               OBJECT = OrganizationTelephoneContainer
                  Data_Location = "NONE"
                  Mandatory = "TRUE"
                  CLASS = "1"
                  OBJECT = TelephoneNumberType
                     Mandatory = "TRUE"
                     CLASS = "1"
                     Data_Location = "MCF"
                     NUM_VAL = 1
                     TYPE = "STRING"
                     Value = "Voice"
                  END_OBJECT = TelephoneNumberType
                  OBJECT = TelephoneNumber
                     Mandatory = "TRUE"
                     CLASS = "1"
                     Data_Location = "MCF"
                     NUM_VAL = 1
                     TYPE = "STRING"
                     Value = "301-614-5224"
                  END_OBJECT = TelephoneNumber
               END_OBJECT = OrganizationTelephoneContainer
               OBJECT = OrganizationTelephoneContainer
                  Data_Location = "NONE"
                  Mandatory = "TRUE"
                  CLASS = "2"
                  OBJECT = TelephoneNumberType
                     Mandatory = "TRUE"
                     CLASS = "2"
                     Data_Location = "MCF"
                     NUM_VAL = 1
                     TYPE = "STRING"
                     Value = "Voice"
                  END_OBJECT = TelephoneNumberType
                  OBJECT = TelephoneNumber
                     Mandatory = "TRUE"
                     CLASS = "2"
                     Data_Location = "MCF"
                     NUM_VAL = 1
                     TYPE = "STRING"
                     Value = "1-800-257-6151"
                  END_OBJECT = TelephoneNumber
               END_OBJECT = OrganizationTelephoneContainer
            END_GROUP = OrganizationTelephone
            GROUP = ContactOrganizationAddress
               CLASS = "1"
               OBJECT = ContactOrganizationAddressContainer
                  Data_Location = "NONE"
                  Mandatory = "TRUE"
                  CLASS = "1"
                  OBJECT = Country
                     Mandatory = "TRUE"
                     CLASS = "1"
                     Data_Location = "MCF"
                     NUM_VAL = 1
                     TYPE = "STRING"
                     Value = "USA"
                  END_OBJECT = Country
                  OBJECT = StateProvince
                     Mandatory = "TRUE"
                     CLASS = "1"
                     Data_Location = "MCF"
                     NUM_VAL = 1
                     TYPE = "STRING"
                     Value = "MD"
                  END_OBJECT = StateProvince
                  OBJECT = StreetAddress
                     Mandatory = "TRUE"
                     CLASS = "1"
                     Data_Location = "MCF"
                     NUM_VAL = 1
                     TYPE = "STRING"
                     Value = "NASA Goddard Space Flight Center"
                  END_OBJECT = StreetAddress
                  OBJECT = PostalCode
                     Mandatory = "TRUE"
                     CLASS = "1"
                     Data_Location = "MCF"
                     NUM_VAL = 1
                     TYPE = "STRING"
                     Value = "20771"
                  END_OBJECT = PostalCode
                  OBJECT = City
                     Mandatory = "TRUE"
                     CLASS = "1"
                     Data_Location = "MCF"
                     NUM_VAL = 1
                     TYPE = "STRING"
                     Value = "Greenbelt"
                  END_OBJECT = City
               END_OBJECT = ContactOrganizationAddressContainer
            END_GROUP = ContactOrganizationAddress
            OBJECT = HoursofService
               Mandatory = "TRUE"
               CLASS = "1"
               Data_Location = "MCF"
               NUM_VAL = 1
               TYPE = "STRING"
               Value = "9 AM to 5 PM ET"
            END_OBJECT = HoursofService
            OBJECT = Role
               Mandatory = "TRUE"
               CLASS = "1"
               Data_Location = "MCF"
               NUM_VAL = 1
               TYPE = "STRING"
               Value = "Archive"
            END_OBJECT = Role
            OBJECT = ContactOrganizationName
               Mandatory = "TRUE"
               CLASS = "1"
               Data_Location = "MCF"
               NUM_VAL = 1
               TYPE = "STRING"
               Value = "GSFC DAAC Helpdesk"
            END_OBJECT = ContactOrganizationName
            OBJECT = ContactInstructions
               Mandatory = "TRUE"
               CLASS = "1"
               Data_Location = "MCF"
               NUM_VAL = 1
               TYPE = "STRING"
               Value = "None"
            END_OBJECT = ContactInstructions
         END_OBJECT = ContactOrganizationContainer
      END_GROUP = ContactOrganization
   END_GROUP = Contact
   GROUP = CSDTDescription
      OBJECT = CSDTComments
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "These are CCSDS packets"
      END_OBJECT = CSDTComments
      OBJECT = PrimaryCSDT
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "CCSDS Packets"
      END_OBJECT = PrimaryCSDT
      OBJECT = IndirectReference
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "None"
      END_OBJECT = IndirectReference
      OBJECT = Implementation
         Mandatory = "TRUE"
         Data_Location = "MCF"
         NUM_VAL = 1
         TYPE = "STRING"
         Value = "CCSDS"
      END_OBJECT = Implementation
   END_GROUP = CSDTDescription
   GROUP = AdditionalAttributes
      OBJECT = AdditionalAttributesContainer
         Data_Location = "NONE"
         Mandatory = "TRUE"
         CLASS = "1"
         OBJECT = AdditionalAttributeDescription
            Mandatory = "TRUE"
            CLASS = "1"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "STRING"
            Value = "FDD Definitive Attitude orbit number corresponding to the first data record"
         END_OBJECT = AdditionalAttributeDescription
         OBJECT = AdditionalAttributeDatatype
            Mandatory = "TRUE"
            CLASS = "1"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "STRING"
            Value = "int"
         END_OBJECT = AdditionalAttributeDatatype
         OBJECT = AdditionalAttributeName
            Mandatory = "TRUE"
            CLASS = "1"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "STRING"
            Value = "FDDStartOrbitNumber"
         END_OBJECT = AdditionalAttributeName
      END_OBJECT = AdditionalAttributesContainer
      OBJECT = AdditionalAttributesContainer
         Data_Location = "NONE"
         Mandatory = "TRUE"
         CLASS = "2"
         OBJECT = AdditionalAttributeDescription
            Mandatory = "TRUE"
            CLASS = "2"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "STRING"
            Value = "FDD Definitive Attitude orbit number corresponding to the last data record"
         END_OBJECT = AdditionalAttributeDescription
         OBJECT = AdditionalAttributeDatatype
            Mandatory = "TRUE"
            CLASS = "2"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "STRING"
            Value = "int"
         END_OBJECT = AdditionalAttributeDatatype
         OBJECT = AdditionalAttributeName
            Mandatory = "TRUE"
            CLASS = "2"
            Data_Location = "MCF"
            NUM_VAL = 1
            TYPE = "STRING"
            Value = "FDDEndOrbitNumber"
         END_OBJECT = AdditionalAttributeName
      END_OBJECT = AdditionalAttributesContainer
   END_GROUP = AdditionalAttributes
END_GROUP = COLLECTIONMETADATA
GROUP = ARCHIVEDMETADATA
END_GROUP = ARCHIVEDMETADATA
END
EOF
close MET;

# Determine filesizes for the creation of the dummy DN
#

my $dumsize = (-s $dumfile);
my $metasize  = (-s $metfile);

# 
# Create the dummy DN
#

my $dnpathname = "../../receive_dn/$dnfile";
my $ftpdir = "$WORKDIR";
$ftpdir = dirname($ftpdir);
$ftpdir = dirname($ftpdir);
$ftpdir = $ftpdir . "/DATA/INPUT";
open(DN,">$dnpathname") or 
    S4P::perish(112, "main: Could not open DN file: [$dnpathname]");

print DN <<EOF;
ORDERID: NONE
REQUESTID: 979769952
USERSTRING: woohoo2001
FINISHED: 01/17/2001 18:17:18
 
MEDIATYPE: FtpPush
FTPHOST: dummy.ecs.nasa.gov
FTPDIR: $ftpdir
MEDIA 1 of 1
MEDIAID: 
 
        GRANULE: UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]:19:SC:MOD000.001:48405
        ESDT: $ESDT.001
 
                FILENAME: $basename
                FILESIZE: $dumsize
 
                FILENAME: $basename.met
                FILESIZE: $metasize
EOF
close DN;

print "PGE_VERSION = ",$VERSION,"\nOrbit number = ",$orbit;
my $templatefile = "../../ALGORITHMS/$PGE/$VERSION/GDAAC.$PGE.pcf.tpl";
open(ITPL,"<$templatefile") or 
    S4P::perish(120, "main: Could not open Baseline PCF template: [$templatefile]");
my $templatefileI = "../../ALGORITHMS/$PGEI/$VERSION/GDAAC.$PGEI.pcf.tpl";
open(OTPL,">$templatefileI") or 
    S4P::perish(113, "main: Could not open Init PCF template: [$templatefileI]");

while(<ITPL>){
    chomp;
    if(/^\#998/){
        print OTPL "998|Initial Orbit Number|$orbit\n";
    }
    elsif(/^999/){
        print OTPL "999|Profile ID|2\n";
    }
    else{
        print OTPL "$_\n";
    }
}
close OTPL;
close ITPL;

exit;

}
sub getdatetime{

my $dum = "";
my $file = "";
my $granflag=0;
my $stamp = "";
my $time = "";

    ($dum,$file,$stamp) = split(/\./,$platform);
    print "Opening: DO.$file.$stamp\n";
    open(GRANF,"<$WORKDIR/DO.$file.$stamp") or
        S4P::perish(121, "getdatetime: Could not open file: [DO.$file.$stamp]");
    while(<GRANF>){
        chomp;
        if(/DATA_START=/){
            $dum = substr($_,12);
            ($startdate,$time) = split(/T/,$dum);
            $starttime = substr($time,0,-2);
            print "START: $startdate $starttime\n";
        }
        if(/DATA_END=/){
            $dum = substr($_,10);
            ($enddate,$time) = split(/T/,$dum);
            $endtime   = substr($time,0,-2);
            $granflag=1;
        }
        last if $granflag;
    }
}
