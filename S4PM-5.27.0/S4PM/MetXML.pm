=head1 NAME

MetXML.pm - a module for manipulating XML metadata files

=head1 SYNOPSIS

=for roff
.nf

generate_boundingbox_xml($parmfile, $xmlfile);

generate_gpolygon_xml($parmfile, $xmlfile);

($ShortName, $VersionID, $RangeBeginningDate, $RangeBeginningTime,
    $ProductionDateTime, $Platform, $Suffix) = 
    parse_metadata_from_s4pm_filename($filename);

$xmlstr = make_basic_metadata($data_file, $input_metadata_file);

get_odl_metadata($metfile, $attribute);

=head1 DESCRIPTION

=over 4

=item generate_boundingbox_xml

This subroutine generates a basic XML file named $xmlfile. The spatial data
is defined with a bounding box. The $parmfile is a parameter file that 
contains exactly one line (example below) of tab delimited attributes that
are to go into the XML file. The attributes must be in this order:

ShortName

VersionID

FileName

SizeMB (divide bytes by 1024 to get MB)

LocalGranuleID

ProductionDateTime (in format as: 2004-02-29T00:03:22.0000Z)

RangeEndingTime (in format as: 12:05:05.0000)

RangeEndingDate (in format as: 2004-02-12)

RangeBeginningTime (in format as: 12:05:05.0000)

RangeBeginningDate (in format as: 2004-02-12)

SouthBoundingCoordinate (degrees)

WestBoundingCoordinate (degress)

NorthBoundingCoordinate (degrees)

EastBoundingCoordinate (degrees)


Example:

AIRB2CCF	2	AIRS.2004.02.12.120.L2.CC.v3.0.10.0.G04044133436.hdf	100	AIRS.2004.02.12.120.L2.CC.v3.0.10.0.G04044133436.hdf	2004-02-29T00:03:22.00000Z	12:05:26.0000000	2004-02-12	11:59:26.0000000	2004-02-12	8.7219	-164.4407	30.0625	-144.0025

=item generate_gpolygonbox_xml

This subroutine generates a basic XML file named $xmlfile. The spatial data
is defined with a 4-point G-polygon. The $parmfile is a parameter file that 
contains exactly one line (example below) of tab delimited attributes that
are to go into the XML file. The attributes must be in this order:

ShortName

VersionID

FileName

SizeMB (divide bytes by 1024 to get MB)

LocalGranuleID

ProductionDateTime (in format as: 2004-02-29T00:03:22.0000Z)

RangeEndingTime (in format as: 12:05:05.0000)

RangeEndingDate (in format as: 2004-02-12)

RangeBeginningTime (in format as: 12:05:05.0000)

RangeBeginningDate (in format as: 2004-02-12)

PointLongitude1 (degrees)

PointLatitude1 (degrees)

PointLongitude2 (degrees)

PointLatitude2 (degrees)

PointLongitude3 (degrees)

PointLatitude3 (degrees)

PointLongitude4 (degrees)

PointLatitude4 (degrees)

Example:

RMT021KM	1	RMT021KM.A2004015.1426.001.2004015150552.hdf	100	RMT021KM.A2004015.1426.001.2004015150552.hdf	2004-01-15T15:05:52.00000Z	14:28:37.0000000	2004-01-15	14:26:07.0000000	2004-01-15	-65.9000	39.4784	-39.8333	35.6798	-43.2433	26.8958	-67.1457	30.2549

=item parse_metadata_from_s4pm_filename

This is a convenience routine for pulling out some metadata items from the
S4PM file name. Usually, it is used to then create an XML file. This function
assumes that the file name complies with the S4PM standard:

<ShortName>.<Platform><YYYY><JJJ>.<HH><MM>.<VersionID>.<yyyyjjjhhmmss>.hdf

where YYYY, JJJ, HH, and MM refer to the collection time year, day of year,
hour, and minute; and yyyyjjjhhmmss refer to the production date and time.

This function returns the quantities formatted appropriately for writing
into an XML file. That is, ProductionDateTime is returned in the form:

2002-02-29T00:03:22.0000Z

and RangeBeginningDate and RangeBeginningTime are returned in the forms:

2002-02-12 and 12:05:26.0000

=item make_basic_metadata

Generate the basic metadata using only the S4PM data file path and a metadata
file from the main input granule.  This routine returns an XML string
with metadata obtained in the following manner:
  LocalGranuleID:     basename(filename)
  File:               basename(filename)
  SizeMB:             -s fullpath of data file
  ShortName:          parsed from S4PM data filename
  VersionID:          parsed from S4PM data filename
  ProductionDateTime: parsed from S4PM data filename
  BeginningDateTime:  obtained from input granule metadata
  EndingDateTime:     obtained from input granule metadata
  GPolygon:           obtained from input granule metadata

N.B.: BoundingBox is not yet supported.

=item get_odl_metadata

Given the metadata ODL file name and an attribute (case-sensitive), this 
routine will return a list of values for that attribute. Enclosing quotation
marks will be removed, but not other translations are done. Note that some
attributes (e.g. ShortName) are in typical metadata files twice. This 
subroutine will return all values found.

=back

=head1 AUTHOR

Stephen Berrick, NASA/GSFC, Code 610.2

=cut

################################################################################
# MetXML.pm,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

package S4PM::MetXML;
use S4P;
use S4P::MetFile;
use S4PM;
use File::Basename;
use strict;
1;

sub generate_boundingbox_xml {

    my ($namefile, $outfile) = @_;

    my @xmlshort = ();
    my @xmlversion = ();
    my @xmlgranule = ();
    my @xmlsizeMB = ();
    my @xmllocalgran = ();
    my @xmlproducttd = ();
    my @xmlendtime = ();
    my @xmlenddate = ();
    my @xmlbegintime = ();
    my @xmlbegindate = ();
    my @xmlsouthb = ();
    my @xmlwestb = ();
    my @xmlnorthb = ();
    my @xmleastb = ();

    open(NAMEFILE, $namefile) or S4P::perish(10, "generate_boundingbox_xml: Failed to open parameter file: $namefile: $!");

    my $numnames = 0;

    my $line;
    while (!eof(NAMEFILE)){
        $line = <NAMEFILE>;
        $line =~ s/\"//g;

        ($xmlshort[$numnames],$xmlversion[$numnames],$xmlgranule[$numnames],$xmlsizeMB[$numnames],$xmllocalgran[$numnames],$xmlproducttd[$numnames],$xmlendtime[$numnames],$xmlenddate[$numnames],$xmlbegintime[$numnames],$xmlbegindate[$numnames],$xmlsouthb[$numnames],$xmlwestb[$numnames],$xmlnorthb[$numnames],$xmleastb[$numnames]) = split(/\t/, $line);


        if ($xmlshort[$numnames] eq "\n"){
            next;
        }

        $xmlshort[$numnames]=strip($xmlshort[$numnames],"b");

        $xmlversion[$numnames]=strip($xmlversion[$numnames],"b");
        $xmlgranule[$numnames]=strip($xmlgranule[$numnames],"b");
        $xmlsizeMB[$numnames]=strip($xmlsizeMB[$numnames],"b");
        $xmlproducttd[$numnames]=strip($xmlproducttd[$numnames],"b");
        $xmlendtime[$numnames]=strip($xmlendtime[$numnames],"b");
        $xmlenddate[$numnames]=strip($xmlenddate[$numnames],"b");
        $xmlbegintime[$numnames]=strip($xmlbegintime[$numnames],"b");
        $xmlbegindate[$numnames]=strip($xmlbegindate[$numnames],"b");
        $xmlsouthb[$numnames]=strip($xmlsouthb[$numnames],"b");
        $xmlwestb[$numnames]=strip($xmlwestb[$numnames],"b");
        $xmlnorthb[$numnames]=strip($xmlnorthb[$numnames],"b");
        $xmleastb[$numnames]=strip($xmleastb[$numnames],"b");
        ++$numnames;
    }

    if (!close(NAMEFILE)){
      die "** Cannot close \'$namefile\': $! **\n";
    }

    if ($numnames == 0){
      exit(1);
    }

    my $descfile = read_boundingbox_template();
    for (my $l = 0; $l < $numnames; ++$l){
        print "$outfile\n";

        if (!open(OUTFILE,">".$outfile)){
          die "** Cannot open \'$outfile\': $! **\n";
        }

        my @lines = split(/\n/, $descfile);
        my $indent;
        foreach my $line ( @lines ) {
            $line .= "\n";
            my $tmpline = strip($line,"b");
            if ($tmpline =~ /^<ShortName>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<ShortName>" . $xmlshort[$l] . "</ShortName>\n";
            } elsif ($tmpline =~ /^<VersionID>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<VersionID>" . $xmlversion[$l] . "</VersionID>\n";
            } elsif ($tmpline =~ /^<File type="Science">/){
              $indent = index($line,"<");
              $line=" " x $indent . "<File type=\"Science\">" . $xmlgranule[$l] . "</File>\n";
            } elsif ($tmpline =~ /^<SizeMB>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<SizeMB>" . $xmlsizeMB[$l] . "</SizeMB>\n";
            } elsif ($tmpline =~ /^<LocalGranuleID>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<LocalGranuleID>" . $xmllocalgran[$l] . "</LocalGranuleID>\n";
            } elsif ($tmpline =~ /^<ProductionDateTime>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<ProductionDateTime>" . $xmlproducttd[$l] . "</ProductionDateTime>\n";
            } elsif ($tmpline =~ /^<RangeEndingTime>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<RangeEndingTime>" . $xmlendtime[$l] . "</RangeEndingTime>\n";
            } elsif ($tmpline =~ /^<RangeEndingDate>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<RangeEndingDate>" . $xmlenddate[$l] . "</RangeEndingDate>\n";
            } elsif ($tmpline =~ /^<RangeBeginningTime>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<RangeBeginningTime>" . $xmlbegintime[$l] . "</RangeBeginningTime>\n";
            } elsif ($tmpline =~ /^<RangeBeginningDate>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<RangeBeginningDate>" . $xmlbegindate[$l] . "</RangeBeginningDate>\n";
            } elsif ($tmpline =~ /^<SouthBoundingCoordinate>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<SouthBoundingCoordinate>" . $xmlsouthb[$l] . "</SouthBoundingCoordinate>\n";
            } elsif ($tmpline =~ /^<WestBoundingCoordinate>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<WestBoundingCoordinate>" . $xmlwestb[$l] . "</WestBoundingCoordinate>\n";
            } elsif ($tmpline =~ /^<NorthBoundingCoordinate>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<NorthBoundingCoordinate>" . $xmlnorthb[$l] . "</NorthBoundingCoordinate>\n";
            } elsif ($tmpline =~ /^<EastBoundingCoordinate>/){
              $indent = index($line,"<");
              $line=" " x $indent . "<EastBoundingCoordinate>" . $xmleastb[$l] . "</EastBoundingCoordinate>\n";
          }
          print OUTFILE $line;
    }

    if (!close(OUTFILE)){
      die "** Cannot close \'$outfile\': $! **\n";
    }
   }
}

sub squash{
  my($str);
  ($str)=@_;
 
  $str =~ s/\s+//g;
 
  return($str);
}

#
# strip blanks subroutine
#
sub strip{
  my($str,$place);
  ($str,$place)=@_;
 
  if ($place =~ /^b/i){                # both
    $str =~ s/^\s*(.*?)\s*$/$1/;
  } elsif ($place =~ /^l/i){           # leading
    $str =~ s/^\s*(.*)/$1/;
  } else {                             # trailing
    $str =~ s/(.*?)\s*$/$1/;
  }
 
  return($str);
}

sub read_boundingbox_template {

    my $template_str = <<EOF;
<?xml version="1.0" encoding="UTF-8"?>

<GranuleMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="NonEcsGranuleMetadata.xsd">

    <SchemaVersion>1.0</SchemaVersion>
    <DataCenterId>GSF</DataCenterId>
            <GranuleMetaData>
                <CollectionMetaData>
                    <ShortName>RMT021KM</ShortName>
                    <VersionID>1</VersionID>
                </CollectionMetaData>
                <DataFilesContainer>
                  <File type="Science">RMT021KM.A2003333.1506.001.2003333170332.hdf</File>
                </DataFilesContainer>
                <DataGranule>
                    <SizeMB>170.89099999999999</SizeMB>
                    <LocalGranuleID>RMT021KM.A2003333.1511.001.2003333170321.hdf</LocalGranuleID>
                    <ProductionDateTime>2003-11-29T17:03:22.00000Z</ProductionDateTime>
                </DataGranule>
                <RangeDateTime>
                    <RangeEndingTime>15:09:25.0000000</RangeEndingTime>
                    <RangeEndingDate>2003-11-29</RangeEndingDate>
                    <RangeBeginningTime>15:06:55.0000000</RangeBeginningTime>
                    <RangeBeginningDate>2003-11-29</RangeBeginningDate>
                </RangeDateTime>
                <SpatialDomainContainer>
                    <HorizontalSpatialDomainContainer>
                        <BoundingRectangle>
                            <SouthBoundingCoordinate>-79.058</SouthBoundingCoordinate>
                            <WestBoundingCoordinate>65.175</WestBoundingCoordinate>
                            <NorthBoundingCoordinate>-53.9859</NorthBoundingCoordinate>
                            <EastBoundingCoordinate>123.2112</EastBoundingCoordinate>
                        </BoundingRectangle>
                    </HorizontalSpatialDomainContainer>
                </SpatialDomainContainer>
            </GranuleMetaData>
</GranuleMetaDataFile>
EOF

    return $template_str;
}

sub parse_metadata_from_s4pm_filename {

    my $fn = shift;

    my ($ShortName, $Platform, $begin, $VersionID, $prod_time, $Suffix) =
        S4PM::parse_s4pm_filename($fn);

    my $cyear = substr($begin, 0, 4);
    my $cdoy  = substr($begin, 4, 3);
    my $chour = substr($begin, 7, 2);
    my $cmin  = substr($begin, 9, 2);

    my $pyear = substr($prod_time, 0, 4);
    my $pdoy  = substr($prod_time, 4, 3);
    my $phour = substr($prod_time, 7, 2);
    my $pmin  = substr($prod_time, 9, 2);
    my $psec  = substr($prod_time, 11, 2);

    my ($new_cyear, $new_cmonth, $new_cday) = S4P::TimeTools::doy_to_ymd($cdoy, $cyear);
    my ($new_pyear, $new_pmonth, $new_pday) = S4P::TimeTools::doy_to_ymd($pdoy, $pyear);
    if ( length($new_cmonth) == 1 ) { $new_cmonth = "0" . $new_cmonth; }
    if ( length($new_cday)   == 1 ) { $new_cday = "0" . $new_cday; }
    if ( length($new_pmonth) == 1 ) { $new_pmonth = "0" . $new_pmonth; }
    if ( length($new_pday)   == 1 ) { $new_pday = "0" . $new_pday; }

    my $RangeBeginningDate = join("-", $new_cyear, $new_cmonth, $new_cday);
    my $csec = "00.0000";
    my $RangeBeginningTime = join(":", $chour, $cmin, $csec);
    my $ProductionDateTime = $new_pyear . "-" . $new_pmonth . "-" . $new_pday . "T" . $phour . ":" . $pmin . ":" . $psec . ".0000Z";

    return $ShortName, $VersionID, $RangeBeginningDate, $RangeBeginningTime, $ProductionDateTime, $Platform, $Suffix;

}

sub generate_gpolygon_xml {

    my ($namefile, $outfile) = @_;

    my @xmlshort = ();
    my @xmlversion = ();
    my @xmlgranule = ();
    my @xmlsizeMB = ();
    my @xmllocalgran = ();
    my @xmlproducttd = ();
    my @xmlendtime = ();
    my @xmlenddate = ();
    my @xmlbegintime = ();
    my @xmlbegindate = ();
    my @xmllongf = ();
    my @xmllatf = ();
    my @xmllongs = ();
    my @xmllats = ();
    my @xmllongt = ();
    my @xmllatt = ();
    my @xmllongfth = ();
    my @xmllatfth = ();
  
    open(NAMEFILE, $namefile) or S4P::perish(10, "generate_basic_xml: Failed to open parameter file: $namefile: $!");

    my $numnames = 0;

    my $line;
    while (!eof(NAMEFILE)){
        $line=<NAMEFILE>;
        $line =~ s/\"//g;

        ($xmlshort[$numnames],$xmlversion[$numnames],$xmlgranule[$numnames],$xmlsizeMB[$numnames],$xmllocalgran[$numnames],$xmlproducttd[$numnames],$xmlendtime[$numnames],$xmlenddate[$numnames],$xmlbegintime[$numnames],$xmlbegindate[$numnames],$xmllongf[$numnames],$xmllatf[$numnames],$xmllongs[$numnames],$xmllats[$numnames],$xmllongt[$numnames],$xmllatt[$numnames],$xmllongfth[$numnames],$xmllatfth[$numnames]) = split(/\t/, $line);


        if ($xmlshort[$numnames] eq "\n"){
            next;
        }

        $xmlshort[$numnames] = strip($xmlshort[$numnames],"b");

        $xmlversion[$numnames]=strip($xmlversion[$numnames],"b");
        $xmlgranule[$numnames]=strip($xmlgranule[$numnames],"b");
        $xmlsizeMB[$numnames]=strip($xmlsizeMB[$numnames],"b");
        $xmlproducttd[$numnames]=strip($xmlproducttd[$numnames],"b");
        $xmlendtime[$numnames]=strip($xmlendtime[$numnames],"b");
        $xmlenddate[$numnames]=strip($xmlenddate[$numnames],"b");
        $xmlbegintime[$numnames]=strip($xmlbegintime[$numnames],"b");
        $xmlbegindate[$numnames]=strip($xmlbegindate[$numnames],"b");
        $xmllongf[$numnames]=strip($xmllongf[$numnames],"b");
        $xmllatf[$numnames]=strip($xmllatf[$numnames],"b");
        $xmllongs[$numnames]=strip($xmllongs[$numnames],"b");
        $xmllats[$numnames]=strip($xmllats[$numnames],"b");
        $xmllongt[$numnames]=strip($xmllongt[$numnames],"b");
        $xmllatt[$numnames]=strip($xmllatt[$numnames],"b");
        $xmllongfth[$numnames]=strip($xmllongfth[$numnames],"b");
        $xmllatfth[$numnames]=strip($xmllatfth[$numnames],"b");
        ++$numnames;
    }

    if (!close(NAMEFILE)){
        die "** Cannot close \'$namefile\': $! **\n";
    }

    if ($numnames == 0){
        exit(1);
    }

    my $descfile = read_gpolygon_template();
    for (my $l = 0; $l < $numnames; ++$l){
        print "$outfile\n";

        if (!open(OUTFILE,">".$outfile)){
            die "** Cannot open \'$outfile\': $! **\n";
        }

        my @lines = split(/\n/, $descfile);
        my $indent;
        foreach my $line ( @lines ) {
            $line .= "\n";
            my $tmpline=strip($line,"b");
            if ($tmpline =~ /^<ShortName>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<ShortName>" . $xmlshort[$l] . "</ShortName>\n";
            } elsif ($tmpline =~ /^<VersionID>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<VersionID>" . $xmlversion[$l] . "</VersionID>\n";
            } elsif ($tmpline =~ /^<File type="Science">/){
              $indent=index($line,"<");
              $line=" " x $indent . "<File type=\"Science\">" . $xmlgranule[$l] . "</File>\n";
            } elsif ($tmpline =~ /^<SizeMB>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<SizeMB>" . $xmlsizeMB[$l] . "</SizeMB>\n";
            } elsif ($tmpline =~ /^<LocalGranuleID>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<LocalGranuleID>" . $xmllocalgran[$l] . "</LocalGranuleID>\n";
            } elsif ($tmpline =~ /^<ProductionDateTime>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<ProductionDateTime>" . $xmlproducttd[$l] . "</ProductionDateTime>\n";
            } elsif ($tmpline =~ /^<RangeEndingTime>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<RangeEndingTime>" . $xmlendtime[$l] . "</RangeEndingTime>\n";
            } elsif ($tmpline =~ /^<RangeEndingDate>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<RangeEndingDate>" . $xmlenddate[$l] . "</RangeEndingDate>\n";
            } elsif ($tmpline =~ /^<RangeBeginningTime>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<RangeBeginningTime>" . $xmlbegintime[$l] . "</RangeBeginningTime>\n";
            } elsif ($tmpline =~ /^<RangeBeginningDate>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<RangeBeginningDate>" . $xmlbegindate[$l] . "</RangeBeginningDate>\n";
            } elsif ($tmpline =~ /^<PointLongitude1>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<PointLongitude>" . $xmllongf[$l] . "</PointLongitude>\n";
            } elsif ($tmpline =~ /^<PointLatitude1>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<PointLatitude>" . $xmllatf[$l] . "</PointLatitude>\n";
            } elsif ($tmpline =~ /^<PointLongitude2>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<PointLongitude>" . $xmllongs[$l] . "</PointLongitude>\n";
            } elsif ($tmpline =~ /^<PointLatitude2>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<PointLatitude>" . $xmllats[$l] . "</PointLatitude>\n";
            } elsif ($tmpline =~ /^<PointLongitude3>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<PointLongitude>" . $xmllongt[$l] . "</PointLongitude>\n";
            } elsif ($tmpline =~ /^<PointLatitude3>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<PointLatitude>" . $xmllatt[$l] . "</PointLatitude>\n";
            } elsif ($tmpline =~ /^<PointLongitude4>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<PointLongitude>" . $xmllongfth[$l] . "</PointLongitude>\n";
            } elsif ($tmpline =~ /^<PointLatitude4>/){
              $indent=index($line,"<");
              $line=" " x $indent . "<PointLatitude>" . $xmllatfth[$l] . "</PointLatitude>\n";
        }
        print OUTFILE $line;
    }

  if (!close(OUTFILE)){
    die "** Cannot close \'$outfile\': $! **\n";
  }
 }
}

sub read_gpolygon_template {

    my $template_str = <<EOF;
<?xml version="1.0" encoding="UTF-8"?>

<GranuleMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="NonEcsGranuleMetadata.xsd">

    <SchemaVersion>1.0</SchemaVersion>
    <DataCenterId>GSF</DataCenterId>
            <GranuleMetaData>
                <CollectionMetaData>
                    <ShortName>RMT021KM</ShortName>
                    <VersionID>1</VersionID>
                </CollectionMetaData>
                <DataFilesContainer>
                  <File type="Science">RMT021KM.A2003333.1506.001.2003333170332.hdf</File>
                </DataFilesContainer>
                <DataGranule>
                    <SizeMB>170.89099999999999</SizeMB>
                    <LocalGranuleID>RMT021KM.A2003333.1511.001.2003333170321.hdf</LocalGranuleID>
                    <ProductionDateTime>2003-11-29T17:03:22.00000Z</ProductionDateTime>
                </DataGranule>
                <RangeDateTime>
                    <RangeEndingTime>15:09:25.0000000</RangeEndingTime>
                    <RangeEndingDate>2003-11-29</RangeEndingDate>
                    <RangeBeginningTime>15:06:55.0000000</RangeBeginningTime>
                    <RangeBeginningDate>2003-11-29</RangeBeginningDate>
                </RangeDateTime>
                <SpatialDomainContainer>
                    <HorizontalSpatialDomainContainer>
                        <GPolygon>
                            <Boundary>
                                <Point>
                                    <PointLongitude1>-67.0112</PointLongitude>
                                    <PointLatitude1>30.4652</PointLatitude>
                                </Point>
                                <Point>
                                    <PointLongitude2>-43.4692</PointLongitude>
                                    <PointLatitude2>27.0234</PointLatitude>
                                </Point>
                                <Point>
                                    <PointLongitude3>-44.0535</PointLongitude>
                                    <PointLatitude3>25.2233</PointLatitude>
                                </Point>
                                <Point>
                                    <PointLongitude4>-67.3249</PointLongitude>
                                    <PointLatitude4>28.5992</PointLatitude>
                                </Point>
                            </Boundary>
                        </GPolygon>
                    </HorizontalSpatialDomainContainer>
                </SpatialDomainContainer>
            </GranuleMetaData>
</GranuleMetaDataFile>
EOF

     return $template_str;
}
###########################################################################
# make_basic_metadata($data_file, $input_metadata_file);
#   $data_file: data file name (gets ShortName, VersionID, ProductionDateTime, etc.)
#   $input_metadata_file:  Metadata for *input* product (used for spatial and
#       any additional metadata)
#--------------------------------------------------------------------------
# Create a basic metadata XML string, using only the data file and the metadata
# file for an input product
sub make_basic_metadata {
    my ($data_file, $input_metadata_file) = @_;

    # Get spatial and temporal contstraints of the input product
    my ($start, $stop, $ra_latitude, $ra_longitude) = 
        S4P::MetFile::get_spatial_temporal_metadata($input_metadata_file);
    return undef unless $start;

    # Start putting together a %met hash
    my %met;
    $start =~ s/Z//;
    $stop =~ s/Z//;
    ($met{'RangeBeginningDate'}, $met{'RangeBeginningTime'}) = split('[T ]', $start);
    ($met{'RangeEndingDate'}, $met{'RangeEndingTime'}) = split('[T ]', $stop);
    
    # GPolygon template only available for 4 points
    my $i;
    for ($i = 1; $i <= 4; $i++) {
        $met{'PointLatitude' . $i} = $ra_latitude->[$i-1];
        $met{'PointLongitude' . $i} = $ra_longitude->[$i-1];
    }

    # Get shortname, version_id, production time from the filename
    my ($esdt, $platform, $begin, $version, $prod_time, $suffix) = 
        S4PM::parse_s4pm_filename(basename($data_file));
    return undef unless $esdt;
    
    $met{'ShortName'} = $esdt;
    $met{'VersionID'} = $version;
    $met{'ProductionDateTime'} = S4P::TimeTools::yyyydddhhmmss2CCSDSa($prod_time);
    $met{'ProductionDateTime'} =~ tr/TZ/ /d;

    # Get LocalGranuleID and size from data file itself
    $met{'LocalGranuleID'} = basename($data_file);
    $met{'File'} = basename($data_file);
    $met{'SizeMB'} = (-s $data_file) / (1024*1024);

    # Substitute using gpolygon template
    my $xml = read_gpolygon_template();
    my ($rc, $err);
    foreach (keys %met) {
        # Latitude/longitude has asymmetric tag templates: 
        # e.g., <PointLatitude1><PointLatitude>
        if (/PointL(at|ong)itude/) {
            my ($tag1, $tag2) = ($_, $_);
            $tag2 =~ s/itude\d/itude/;
            $met{$_} =~ s/ //g;
            $rc = ($xml =~ s#$tag1>.*?</$tag2>#$tag2>$met{$_}</$tag2>#s);
        }
        # File has an extra attribute, type
        elsif ($_ eq 'File') {
            $rc = ($xml =~ s#$_ type="Science">.*?</$_>#$_ type="Science">$met{$_}</$_>#s);
        }
        else {
            $rc = ($xml =~ s#$_>.*?</$_>#$_>$met{$_}</$_>#s);
        }
        unless ($rc) {
            warn("Error substituting $met{$_} for $_ attribute");
            $err++;
        }
    }
    return $err ? undef : $xml;
}

sub get_odl_metadata {

    my ($file, $attr) = @_;

    my @results = ();

    my $odl = S4P::OdlTree->new ( FILE => $file );

    my @list = $odl->search('NAME' => $attr);

    return undef unless ( scalar(@list) > 0 );

    foreach my $element ( @list ) {
        my $value = $element->getAttribute('VALUE');
        $value =~ s/^"//;
        $value =~ s/"$//;
        push(@results, $value);
    }

    return(@results);

}

