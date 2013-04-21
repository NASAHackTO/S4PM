#!/usr/bin/perl

=head1 NAME

s4pm_l0.pl - parse MODIS Level 0 construction records and files

=head1 SYNOPSIS

s4pm_l0.pl B<-c> I<construction_record> [B<-v>] [B<-w>] [B<-t>] [B<-f>]

s4pm_l0.pl B<-p> I<packet_file> [B<-v>] [B<-s> I<start_pkt>] [B<-e> I<end_pkt>] [B<-a>]

=head1 DESCRIPTION

B<s4pm_l0.pl> parses level 0 construction records and packet files.
For the construction record it parses until it hits a serious problem, like
a filled spare bit, bad CCSDS time code, etc.  This can be overridden by
setting the B<-w> (warn-only) option, but beware.  Once the parsing goes off
the rails, unpredictable results can follow with potentially voluminous output.

It can also parse packet files.  Since this allows a start and stop packet
to be specified, it will continue parsing even when errors are encountered.
The specific errors it detects are bad CCSDS timecodes and checksum mismatches.

To actually SEE the contents of the construction record or packet files, set the
B<-v> (verbose) option.

=head1 ARGUMENTS

=over 4

=item B<-v>

Print (almost) everything.  This prints everything found except the actual
data values in the packet case.

=item B<-t>

Print the start and stop times only (for construction records)
For packets, just prints the start time of the first packet.

=item B<-f>

Print the packet filenames (Construction records only).

=item B<-w>

Warn-only.  Keep trying to parse instead of dying. (Construction records only).

=item B<-s> I<start_packet>

Start parsing at this packet, numbering from 0. (Packet files only).

=item B<-e> I<stop_packet>

Stop parsing at this packet, numbering from 0. (Packet files only).

=item B<-a>

Print spacecraft ancillary data. (Packet files only).

=back

=head1 DIAGNOSTICS

Normally, it will exit with the number of errors found.  If this exceeds 255,
the exit code is set to 255.

=head1 BUGS

It is cavalier about leap seconds, which is to say, it doesn't address them.

Also, it does not attempt to convert NASA PB-5 format times into readable times
(yet).

=head1 AUTHOR

Christopher S Lynnes, NASA/GSFC, Code 610.2

=cut

################################################################################
# s4pm_l0.pl,v 1.3 2007/02/12 15:58:24 lynnes Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

use Math::BigInt;
use Math::BigFloat;
use Getopt::Std;
use strict;
use vars qw($reclen $opt_a $opt_c $opt_f $opt_s $opt_e $opt_p $opt_w $opt_v $opt_t);

getopts('ac:fp:s:e:tvw');
# Set autoflush on
$| = 1;

# Get files to process
my $packet_file = $opt_p;
my $construction_record = $opt_c || $ARGV[0];
usage() if (! $packet_file && ! $construction_record);
my $err = 0;
if ($packet_file) {
    if ($opt_t) {
        $opt_s = 0 unless $opt_s;
        $opt_e = 0 unless $opt_e;
    }
    my $rc = parse_packet($packet_file, $opt_s, $opt_e);
    $err += $rc;
}
if ($construction_record) {
    my $rc = parse_construction_record($construction_record);
    $err += $rc;
}
$err = 255 if $err > 255;
exit($err);

sub parse_packet {
    my $file = shift;
    my $start_packet = shift;
    my $stop_packet = shift;
    $stop_packet = 2000000 if (! defined $stop_packet);
    my $err = 0;

    my ($packet, $pos);
    my ($primary_hdr, $secondary_hdr, $modis_hdr);

    my @cal_types = ('SolarDiffuser','SRCA','Blackbody','Space');
    my @cal_modes = ('Radiometric','Spatial','Spectral','Non-Cal');
    my %pkt_types = (0=>'Day',1=>'Night',2=>'Eng1',4=>'Eng2');

    open IN, $file or die "Cannot open file $file: $!";
    $reclen = -s $file;
    printf "File Size: %d\n", $reclen if ($opt_v);

    local($/)=undef;   # Turn off input record separator, just in case

    # Initialize
    $packet = -1;
    $pos = 0;
    $opt_w = 1;  # So we can bust through CCSDS timecode errors
    my %n_good;
    my %n_bad;
    my $is_good;
    my $pkt_type;
    while ($packet < $stop_packet && $pos < $reclen) {
        $is_good = 1;
        read IN, $primary_hdr, 6;
        read IN, $secondary_hdr, 9;
        my $pkt_len = unpack("n",substr($primary_hdr,4,2));
        read IN, $modis_hdr, 3;
        my $data;
        read IN, $data, $pkt_len - 11;
        $pos += 7 + $pkt_len;
        $packet++;
        next if ($packet < $start_packet);

        my $bits = unpack("B32", $primary_hdr);
        my $pkt_seq_count = bin2dec(substr($bits,18,14));
        my $sequence_flag = bin2dec(substr($bits,16,2));
        printf "Pkt=%d Vers=%s Type=%d SecHdrFlag=%d APID=%d SeqFlag=%d PktSeqCnt=%d PktLen=%d\n",
            $packet, substr($bits,0,1), substr($bits,1,3), substr($bits,4,1),
            bin2dec(substr($bits,5,11)), $sequence_flag,
            $pkt_seq_count, $pkt_len if $opt_v;
        my $sec_pos = 0;
        my $timestamp = ccsds_time($secondary_hdr, \$sec_pos);
        if (! $timestamp) {
            $is_good = 0;
            $err++;
            printf "Packet $packet: CCSDS Timecode Corruption\n";
        }
        elsif ($opt_t) {
            print "$timestamp\n";
        }
        my $sec_hdr_string = unpack("B8", substr($secondary_hdr, $sec_pos, 1));
        my $ql_flag = substr($sec_hdr_string,0,1);
        my $user_flags = substr($sec_hdr_string,1,7);
        $pkt_type = bin2dec(substr($user_flags,0,3));
        my $scan_count = bin2dec(substr($user_flags,3,3));
        my $mirror_side = substr($user_flags,7,1);
        printf "  Time=%s Q/L=%s UserFlags=%s\n  PktType=%d ScanCount=%d MirrorSide=%d\n", 
            $timestamp, $ql_flag, $user_flags, $pkt_type, $scan_count, $mirror_side + 1 if $opt_v;
        if ($pkt_type == 4) {
            if ($opt_a && $sequence_flag == 1) {
                printf "%d: S/C Ancillary Packet\n", $packet;
                $sec_pos = 128 + 6;
                my $pkt_time = ccsds_time($data, \$sec_pos);
                $is_good = 0 if (! $pkt_time);
                printf "  S/C Anc Time: %s\n", $pkt_time;
                $sec_pos++;    # Flag Byte (?)
                $sec_pos += 3; # Time Conversion (?)
                my (%s_c_pos, %s_c_vel);
                $s_c_pos{'x'} = getval($data, \$sec_pos, 8, "l") * .001;
                $s_c_pos{'y'} = getval($data, \$sec_pos, 8, "l") * .001;
                $s_c_pos{'z'} = getval($data, \$sec_pos, 8, "l") * .001;
                $s_c_vel{'x'} = getval($data, \$sec_pos, 8, "l") * .000000001;
                $s_c_vel{'y'} = getval($data, \$sec_pos, 8, "l") * .000000001;
                $s_c_vel{'z'} = getval($data, \$sec_pos, 8, "l") * .000000001;
                
                printf "  S/C Position: x=%.3f m y=%.3f m z=%.3f m\n",
                    $s_c_pos{'x'}, $s_c_pos{'y'}, $s_c_pos{'z'};
                printf "  S/C Velocity: x=%.3f m/s y=%.3f m/s z=%.3f m/s\n",
                    $s_c_vel{'x'}, $s_c_vel{'y'}, $s_c_vel{'z'};
            }
        }
        elsif ($pkt_type == 1) {
            my $modis_hdr_bits = unpack("B24",$modis_hdr);
            my $source_id = substr($modis_hdr_bits,0,1);
            my $source_type = $source_id ? "Cal" : "Earth";
            printf "  SrcId=%s SrcTyp=%s\n", $source_id, $source_type if $opt_v;
            if ($source_type) {
                my $cal_type = bin2dec(substr($modis_hdr_bits,1,2));
                my $cal_mode = bin2dec(substr($modis_hdr_bits,3,2));
                my $cal_frame_count = bin2dec(substr($modis_hdr_bits,6,6));
                printf "  Type=%s Mode=%s FrameCount=%d\n", $cal_types[$cal_type], 
                    $cal_modes[$cal_mode], $cal_frame_count if $opt_v;
            }
            else {
                my $earth_frame_count = bin2dec(substr($modis_hdr_bits,1,11));
                printf "  EarthFrameCount=%d\n", $earth_frame_count if $opt_v;
            }
            my $fpa_aem_config = substr($modis_hdr_bits, 12, 10);
            my $sci_state = substr($modis_hdr_bits,22,1);
            my $sci_abnorm = substr($modis_hdr_bits,23,1);
            printf "  FpaAemConfig=%s SciState=%s SciAbnorm=%s\n", 
                $fpa_aem_config, $sci_state, $sci_abnorm if $opt_v;
        }
        my @data = unpack_12bit_words($data);
        my $n_data = scalar(@data);
        printf "  Data: %6d %6d %6d ... %6d %6d %6d\n", 
            $data[18], $data[19], $data[20], $data[$n_data-3], 
            $data[$n_data-2], $data[$n_data-1] if $opt_v;
        my $pkt_checksum = pop(@data);
        my $checksum = checksum(@data);
        if ($checksum != $pkt_checksum) {
            $is_good = 0;
            printf "Packet %d Checksum Error:  Recorded: %d  Computed: %d\n", 
            $packet, $pkt_checksum, $checksum;
            $err++;
        }
        elsif ($opt_v) {
            printf "Packet %d Checksums:  Recorded: %d  Computed: %d\n", 
            $packet, $pkt_checksum, $checksum;
        }
        $n_good{$pkt_type}++ if ($is_good);
        $n_bad{$pkt_type}++ if (! $is_good);
    }
    foreach $pkt_type(keys %pkt_types) {
        printf "%5.5s packets:  %d Good  %d Bad\n", 
            $pkt_types{$pkt_type}, $n_good{$pkt_type}, $n_bad{$pkt_type};
    }
    return $err;
}
sub checksum {
    use integer;
    my $sum = 0;
    my $val;
    foreach $val(@_) {
        $sum += $val;
        $sum %= 65536;
    }
    $sum >>= 4;
    return $sum;
}
sub unpack_12bit_words {
    use integer;
    my $data = shift;
    my $len = length($data);
    my $nchunks = length($data) / 3;
    my ($i, $pos);
    my ($chunk, @data);
    my (@a, $nibble1, $nibble2, $b);
    for ($i = 0; $i < $nchunks; $i++) {
        $pos = $i * 3;
        @a = unpack('CCC',substr($data,$pos,3));
        $b = $a[1];
        $nibble1 = $b & 0x0f;
        $nibble2 = ($b & 0xf0) >> 4;
        push @data, $nibble2 | ($a[0] << 4);
        push @data, $a[2] | ($nibble1 << 8);
    }
    return @data;
}
sub parse_construction_record {
    my $file = shift;
    open IN, $file or die "Cannot open file $file: $!";
    local($/)=undef;
    my $rec = <IN>;
    close IN;
    my @vals = ();
    my @names = ();
    my $pos = 0;
    my ($i, $j);
    $reclen = length $rec;
    my %cr;

    # Item 1
    push @names, "1: EDOS Major Software Version";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 1, 'C');
    printf "%d\n", (@vals)[-1] if ($opt_v);
    push @names, "1: EDOS Minor Software Version";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 1, 'C');
    printf "%d\n", (@vals)[-1] if ($opt_v);

    # Item 2
    my $ncr_type = getval($rec, \$pos, 1, 'C', 1, 3, "2");
    push @names, "2: Construction Record Type Code";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, $ncr_type;
    printf "%d\n", (@vals)[-1] if ($opt_v);
    push @names, "2: Construction Record Type";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, ('PDS','EDS','EDS')[$ncr_type-1];
    printf "%s\n", (@vals)[-1] if ($opt_v);

    # Item 3
    spare($rec, \$pos, 1, "3");

    # Item 4
    push @names, "4: PDS/EDS Identification";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 36, 'A' x 36);
    printf "%s\n", (@vals)[-1] if ($opt_v);

    # Item 5 / 6
    push @names, "6: Test Flag";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 1, 'b', 0, 1, "6");
    printf "%d\n", (@vals)[-1] if ($opt_v);

    # Item 7
    spare($rec, \$pos, 1, "7-1");
    spare($rec, \$pos, 8, "7-2");

    # Item 8
    my $n_scs = getval($rec, \$pos, 2, 'n');
    push @names, "8: Number of SCS start/stop times";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, $n_scs;
    printf "%d\n", (@vals)[-1] if ($opt_v);

    for ($i = 0; $i < $n_scs; $i++) {
        # Item 8-1
        spare($rec, \$pos, 1, "8-1");

        # Item 8-2
        push @names, "  8-2: SCS $i start";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, pb5_time($rec, \$pos);
        printf "%s\n", (@vals)[-1] if ($opt_v);

        # Item 8-3
        spare($rec, \$pos, 1, "8-3");

        # Item 8-4
        push @names, "  8-4: SCS $i stop";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, pb5_time($rec, \$pos);
        printf "%s\n", (@vals)[-1] if ($opt_v);
    }
    # Item 9
    push @names, "9: Number of octets of EDOS generated fill data";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 8, "I");
    printf "%d\n", (@vals)[-1] if ($opt_v);

    # Item 10
    push @names, "10: Header/actual length discrepancies";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 4, "I");
    printf "%d\n", (@vals)[-1] if ($opt_v);

    # Item 11
    push @names, "11: CCSDS Timecode / 2ndary header of 1st packet";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, ccsds_time($rec, \$pos);
    printf "%s\n", (@vals)[-1] if ($opt_v || $opt_t);

    # Item 12
    push @names, "12: CCSDS Timecode / 2ndary header of Last packet";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, ccsds_time($rec, \$pos);
    printf "%s\n", (@vals)[-1] if ($opt_v || $opt_t);
    exit(0) if ($opt_t && ! $opt_v);

    # Item 13
    spare($rec, \$pos, 1, "13");

    # Item 14
    push @names, "14: ESH date/time annotation of 1st packet";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, pb5_time($rec, \$pos);
    printf "%s\n", (@vals)[-1] if ($opt_v);

    # Item 15
    spare($rec, \$pos, 1, "15");

    # Item 16
    push @names, "16: ESH date/time annotation of last packet";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, pb5_time($rec, \$pos);
    printf "%s\n", (@vals)[-1] if ($opt_v);

    # Item 17
    push @names, "17: Packets from VCDUs corrected by R-S decoding";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 4, "I");
    printf "%d\n", (@vals)[-1] if ($opt_v);

    # Item 18
    push @names, "18: Number of packets";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 4, "I");
    printf "%d\n", (@vals)[-1] if ($opt_v);

    # Item 19
    push @names, "19: Number of octets";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 8, "I");
    printf "%d\n", (@vals)[-1] if ($opt_v);

    # Item 20
    push @names, "20: Packets with SSC discontinuities";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, getval($rec, \$pos, 4, "I");
    printf "%d\n", (@vals)[-1] if ($opt_v);

    # Item 21
    spare($rec, \$pos, 1, "21");

    # Item 22
    push @names, "22: Time of completion";
    printf "%s => ", (@names)[-1] if ($opt_v);
    push @vals, pb5_time($rec, \$pos);
    printf "%s\n", (@vals)[-1] if ($opt_v);

    # Item 23
    spare($rec, \$pos, 7, "23");

    # Item 24
    push @names, "24: Number of APIDs";
    printf "%s => ", (@names)[-1] if ($opt_v);
    my $n_apids = getval($rec, \$pos, 1, "C", 1, 3, "24");
    push @vals, $n_apids;
    printf "%d\n", (@vals)[-1] if ($opt_v);
    for ($i = 0; $i < $n_apids; $i++) {
        # Item 24-1
        spare($rec, \$pos, 1, "24-1");
 
        # Item 24-2
        my $scid_apid = getval($rec, \$pos, 3, "B24");
        push @names, "  24-2: SCID[$i]";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals,  bin2dec(substr($scid_apid, 0, 8));
        printf "%d\n", (@vals)[-1] if ($opt_v);
        push @names, "  24-2: APID[$i]";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, bin2dec(substr($scid_apid, 13, 11));
        printf "%d\n", (@vals)[-1] if ($opt_v);

        # Item 24-3
        push @names, "  24-3: Byte offset to first packet";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, getval($rec, \$pos, 8, "L");
        printf "%d\n", (@vals)[-1] if ($opt_v);

        # Item 24-4
        spare($rec, \$pos, 3, "24-4");

        # Item 24-5
        push @names, "  24-5: Number of VCIDs (1-2)";
        printf "%s => ", (@names)[-1] if ($opt_v);
        my $n_vcids = getval($rec, \$pos, 1, "C", 1, 2, "24-5");
        push @vals, $n_vcids;
        printf "%d\n", (@vals)[-1] if ($opt_v);

        for ($j = 0; $j < $n_vcids; $j++) {
            # Item 24-5.1
            spare($rec, \$pos, 2, "24-5.1");

            # Item 24-5.2
            my $vcdu_id = getval($rec, \$pos, 2, "B16");
            push @names, "    24-5.2: SCID";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, bin2dec(substr($vcdu_id, 2, 8));
            printf "%d\n", (@vals)[-1] if ($opt_v);
            push @names, "    24-5.2: VCID";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, bin2dec(substr($vcdu_id, 10, 6));
            printf "%d\n", (@vals)[-1] if ($opt_v);
        }
        # Item 24-6
        push @names, "  24-6: Packets with SSC Discontinuities (Gaps)";
        printf "%s => ", (@names)[-1] if ($opt_v);
        my $n_gaps = getval($rec, \$pos, 4, "I");
        push @vals, $n_gaps;
        printf "%d\n", (@vals)[-1] if ($opt_v);

        for ($j = 0; $j < $n_gaps; $j++) {
            # Item 24-6.1
            push @names, "    24-6.1: Identity of 1st missing packet SSC";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, getval($rec, \$pos, 4, "I");
            printf "%d\n", (@vals)[-1] if ($opt_v);

            # Item 24-6.2
            push @names, "    24-6.2: Byte offset into dataset to missing packet";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, getval($rec, \$pos, 8, "L");
            printf "%d\n", (@vals)[-1] if ($opt_v);

            # Item 24-6.3
            push @names, "    24-6.3: Number of Packet SSCs missed";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, getval($rec, \$pos, 4, "I");
            printf "%d\n", (@vals)[-1] if ($opt_v);

            # Item 24-6.4
            push @names, "    24-6.4: CCSDS timecode / secondary header, pre-gap";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, ccsds_time($rec, \$pos);
            printf "%s\n", (@vals)[-1] if ($opt_v);

            # Item 24-6.5
            push @names, "    24-6.5: CCSDS timecode / secondary header, post-gap";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, ccsds_time($rec, \$pos);
            printf "%s\n", (@vals)[-1] if ($opt_v);

            # Item 24-6.6
            spare($rec, \$pos, 1, "24-6.6");

            # Item 24-6.7
            push @names, "    24-6.7: ESH timecode / secondary header, pre-gap";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, pb5_time($rec, \$pos);
            printf "%s\n", (@vals)[-1] if ($opt_v);

            # Item 24-6.8
            spare($rec, \$pos, 1, "24-6.8");

            # Item 24-6.9
            push @names, "    24-6.9: ESH timecode / secondary header, post-gap";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, pb5_time($rec, \$pos);
            printf "%s\n", (@vals)[-1] if ($opt_v);
        }
        # Item 24-7
        push @names, "  24-7: Number of entries with fill data";
        printf "%s => ", (@names)[-1] if ($opt_v);
        my $n_fill = getval($rec, \$pos, 4, "I");
        push @vals, $n_fill;
        printf "%d\n", (@vals)[-1] if ($opt_v);

        for ($j = 0; $j < $n_fill; $j++) {
            # Item 24-7.1
            push @names, "    24-7.1: SSC of packet with fill data";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, getval($rec, \$pos, 4, "I");
            printf "%d\n", (@vals)[-1] if ($opt_v);

            # Item 24-7.2
            push @names, "    24-7.2: Offset of packet with fill data";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, getval($rec, \$pos, 8, "L");
            printf "%d\n", (@vals)[-1] if ($opt_v);

            # Item 24-7.3
            push @names, "    24-7.3: Index to first fill octet";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, getval($rec, \$pos, 4, "I");
            printf "%d\n", (@vals)[-1] if ($opt_v);
        }
        # Item 24-8
        push @names, "  24-8: Number of fill octets";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, getval($rec, \$pos, 8, "L");
        printf "%d\n", (@vals)[-1] if ($opt_v);

        # Item 24-9
        push @names, "  24-9: Number of packet length discrepancies";
        printf "%s => ", (@names)[-1] if ($opt_v);
        my $n_pktlen = getval($rec, \$pos, 4, "I");
        push @vals, $n_pktlen;
        printf "%d\n", (@vals)[-1] if ($opt_v);

        for ($j = 0; $j < $n_pktlen; $j++) {
            # Item 24-9.1
            push @names, "    24-9.1: SSC of packet with length discrepancy";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, getval($rec, \$pos, 4, "I");
            printf "%d\n", (@vals)[-1] if ($opt_v);
        }
        # Item 24-10
        push @names, "  24-10: CCSDS Time of secondary header of 1st packet";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, ccsds_time($rec, \$pos);
        printf "%s\n", (@vals)[-1] if ($opt_v);

        # Item 24-11
        push @names, "  24-11: CCSDS Time of secondary header of last packet";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, ccsds_time($rec, \$pos);
        printf "%s\n", (@vals)[-1] if ($opt_v);

        # Item 24-12
        spare($rec, \$pos, 1, "24-12");

        # Item 24-13
        push @names, "  24-13: ESH date/time annotation of 1st packet";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, pb5_time($rec, \$pos);
        printf "%s\n", (@vals)[-1] if ($opt_v);

        # Item 24-14
        spare($rec, \$pos, 1, "24-14");

        # Item 24-15
        push @names, "  24-15: ESH date/time annotation of last packet";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, pb5_time($rec, \$pos);
        printf "%s\n", (@vals)[-1] if ($opt_v);

        # Item 24-16
        push @names, "  24-16: Packets from VCDUs corrected by R-S decoding";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, getval($rec, \$pos, 4, "I");
        printf "%d\n", (@vals)[-1] if ($opt_v);

        # Item 24-17
        push @names, "  24-17: Number of packets in dataset";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, getval($rec, \$pos, 4, "I");
        printf "%d\n", (@vals)[-1] if ($opt_v);

        # Item 24-18
        push @names, "  24-18: Number of octets in dataset";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, getval($rec, \$pos, 8, "L");
        printf "%d\n", (@vals)[-1] if ($opt_v);

        # Item 24-19
        spare($rec, \$pos, 8, "24-19");
    }
    # Item 25
    spare($rec, \$pos, 3, "25");
    # Item 25-1
    push @names, "25-1: Number of files in dataset";
    printf "%s => ", (@names)[-1] if ($opt_v);
    my $n_files = getval($rec, \$pos, 1, "C", 1, 255, "25-1");
    push @vals, $n_files;
    printf "%d\n", (@vals)[-1] if ($opt_v);

    for ($i = 0; $i < $n_files; $i++) {
        # Item 25-2
        push @names, "  25-2: Name of PDS/EDS file";
        printf "%s => ", (@names)[-1] if ($opt_v);
        push @vals, getval($rec, \$pos, 40, "A40");
        printf "%s\n", (@vals)[-1] if ($opt_v);

        # Item 25-3
        spare($rec, \$pos, 3, "25-3");

        # Item 25-4
        push @names, "  25-4: Number of APIDs in file";
        printf "%s => ", (@names)[-1] if ($opt_v);
        my $n_apids = getval($rec, \$pos, 1, "C", 0, 3, "25-4");
        push @vals, $n_apids;
        printf "%s\n", (@vals)[-1] if ($opt_v || $opt_f);

        # Need to go through the loop once for construction record file
        $n_apids = 1 if $n_apids == 0; 
        for ($j = 0; $j < $n_apids; $j++) {
            # Item 25-4.1
            spare($rec, \$pos, 1, "25-4.1");

            # Item 25-4.2
            my $scid_apid = getval($rec, \$pos, 3, "B24");
            push @names, "    25-4.2: SCID[$i]";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals,  bin2dec(substr($scid_apid, 0, 8));
            printf "%d\n", (@vals)[-1] if ($opt_v);
            push @names, "    25-4.2: APID[$i]";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, bin2dec(substr($scid_apid, 13, 11));
            printf "%d\n", (@vals)[-1] if ($opt_v);

            # Item 25-4.3
            push @names, "    25-4.3: CCSDS timecode / secondary header of 1st packet";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, ccsds_time($rec, \$pos);
            printf "%s\n", (@vals)[-1] if ($opt_v);

            # Item 25-4.4
            push @names, "    25-4.4: CCSDS timecode / secondary header of last packet";
            printf "%s => ", (@names)[-1] if ($opt_v);
            push @vals, ccsds_time($rec, \$pos);
            printf "%s\n", (@vals)[-1] if ($opt_v);

            # Item 25-4.5
            spare($rec, \$pos, 4, "25-4.5");
        }
    }
}
##############################################################################
# General use subroutines
##############################################################################
sub getval{
    my ($data, $r_pos, $nbytes, $type, $low, $high, $name) = @_;
    if ($$r_pos >= $reclen) {
        die "Oops! walked off the end of the record!\n";
    }
    my $item = substr($data, $$r_pos, $nbytes);
    my $val = join '', unpack($type, $item);
    $$r_pos += $nbytes;
    check_val($name, $val, $low, $high) if ($name);
    return $val;
}
sub pb5_time {
    my ($data, $r_pos) = @_;
    my $item = substr($data, $$r_pos, 7);
    $$r_pos += 7;
    my $bits = unpack('B56', $item);
    my $flag = substr($bits, 0, 1);
    my $jday = substr ($bits, 1, 14);
    my $secs = substr($bits, 15, 17);
    my $millisecs = substr($bits, 32, 10);
    my $microsecs = substr($bits, 42, 10);
    my $time = join ':',bin2dec($jday), bin2dec($secs), bin2dec($millisecs), bin2dec($microsecs);
    return $time;
}
sub bin2dec {
    my $bin = shift;
    my $length = length($bin);
    substr ($bin, 0,0) = '0' x (32 - $length);
    my $dec = unpack('N',pack("B32", $bin));
    return $dec;
}
sub ccsds_time {
    my ($rec, $r_pos) = @_;
    my @sc_time = unpack "CCCCCCCC", substr($rec, $$r_pos, 8);
    my $secondary_header_id = $sc_time[0] & 128;
    my $err = 0;
    $err++ if !check_val("CCSDS Secondary Header", $secondary_header_id, 0, 0);
    $sc_time[0] %= 128;  # Should be 0, but just in case, do as much as we can
    my $days = ($sc_time[0] * 256) + $sc_time[1];
    my $millisec = (($sc_time[2]*256 + $sc_time[3]) * 256 + $sc_time[4]) * 256 + $sc_time[5];
    my $microsec = ($sc_time[6] * 256) + $sc_time[7];
    $$r_pos += 8;
    $err++ if !check_val("CCSDS Millisec", $millisec, 0, 86401 * 1000 - 1);
    $err++ if !check_val("CCSDS Microsec", $microsec, 0, 999);
    my $on_leap = ($millisec >= 86400000);
    $millisec -= 1000 if $on_leap;

    # Convert to UTC
    my @jd_utc = ((2436204.5+$days), ($millisec+$microsec/1000.)/86400000.0);
    my $day_frac_secs = $jd_utc[1] * 86400 + 0.0000005;
    my $hours = int($day_frac_secs / 3600.);

    if ($hours == 24) {
        $days++;
        $hours = 0;
    }
    my ($year, $month, $day) = calday($jd_utc[0] + 0.5);

    my $minutes = int(($day_frac_secs - $hours * 3600.) / 60.);
    my $seconds = $day_frac_secs - ($hours * 3600.) - ($minutes * 60.);
    my $int_secs = int($seconds);
    my $frac_secs = int (($day_frac_secs - int($day_frac_secs)) * 1000000);
    my $utc = sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%06dZ", 
        $year, $month, $day, $hours, $minutes, $seconds, $frac_secs);
    return $err ? '' : $utc;
}
sub calday {
    use integer;
    my $jday = shift;
    my $l = $jday + 68569;
    my $n = 4 * $l / 146097;
    $l -= (146097*$n + 3)/4;
    my $year = 4000*($l + 1)/1461001;
    $l -= (1461*($year)/4 - 31);
    my $month = 80*$l/2447;
    my $day = $l - 2447 * $month/80;
    $l = $month / 11;
    $month += 2 - (12 * $l);
    $year = 100*($n - 49) + $year + $l;
    return ($year, $month, $day);
}
sub check_val {
    my ($name, $val, $low, $high) = @_;
    my $errstr;
    if ($val < $low) {
        $errstr = "ERROR: $name too low ($val < $low)\n";
    }
    if ($val > $high) {
        $errstr = "ERROR: $name too high ($val > $high)\n";
    }
    if ($errstr) {
        $main::opt_w ? warn $errstr : die $errstr;
    }
    return $errstr ? 0 : 1;
}
sub spare {
    my ($rec, $r_pos, $n_bytes, $item) = @_;
    my $string = substr($rec, $$r_pos, $n_bytes);
    my @bytes = unpack('C' x $n_bytes, $string);
    my $nbytes = scalar(@bytes);
    my $i;
    my $errstr;
    for ($i = 0; $i < $nbytes; $i++) {
        $errstr .= "ERROR: non-zero byte $bytes[$i] found in spares in item $item at byte $i\n" if ($bytes[$i]);
    }
    if ($errstr) {
        $main::opt_w ? warn $errstr : die $errstr;
    }
    $$r_pos += $n_bytes;
    return 1;
}
sub usage {
    die << "EOF";
Usage:  $0 [-w] [-t] -c <construction_record> 
or 
$0 [-s start_pkt] [-e end_pkt] [-v] -p <packet_file>
EOF
}
