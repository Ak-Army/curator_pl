#!/usr/bin/perl
#
# $Id$
#
# Generic parser script used to fetch data
# from logs and generate a properties
#

use 5.006;

use strict;
use warnings;

use Log::Log4perl;
use Time::HiRes;
use Time::y2038;
our $ellapsedTime = [ Time::HiRes::gettimeofday( ) ];

use Elasticsearch;
use Elasticsearch::Bulk;
use Getopt::Long;

use FindBin;
use lib "$FindBin::Bin/modules";

use Storable qw(lock_store lock_retrieve);

use Locker;
use AdjustDate;

# global vars
local our $logger;
Log::Log4perl->init('config/curator_log4perl.conf');
$logger = Log::Log4perl->get_logger('curator');

local our $locker;
$locker = Locker::new('/tmp/.curator.lockfile');

$locker->createAndCheckLock();
Getopt::Long::Configure ("bundling");
my %args = (
	'host'			=> 'localhost',
	'port'			=> 9200,
	'timeout'		=> 30,
	'prefix'		=> [],
	'separator'		=> '.',
	'curation_style'=> 'time',
	'time_unit'		=> 'days',
	'delete'		=> undef,
	'close'			=> undef,
	'bloom'			=> undef,
	'disk_space'	=> undef,
	'max_num_segments'=> 2,
	'optimize'		=> undef
);
GetOptions(\%args, 'host|h=s',
					'port|p=i',
					'timeout|t=i',
					'prefix|p=s@',
					'separator|s=s',
					'curation_style|C=s',
					'time_unit|T=s',
					'delete|d=i',
					'close|c=i',
					'bloom|b=i',
					'disk_space|g=f',
					'max_num_segments|m=i',
					'optimize|o=i',
					'dry-run+',
					'help+'
) or $locker->cleanupAndExit('Argument error...');

if(defined($args{'help'})) {
	usage();
}
validateArgs(\%args);

if(!scalar(@{$args{'prefix'}})) {
	$args{'prefix'} = ['logstash-'];
}

$logger->info("Connect to Elasticsearch - Time:".getEllapsed());
my $client = Elasticsearch->new( servers => "$args{'host'}:$args{'port'}", timeout => $args{'timeout'});

my $info = $client->info();
local our $esVersion = $info->{'version'}{'number'};

my @expiredIndices;
# Delete by space first
if(defined($args{'disk_space'})) {
	$logger->info("Deleting indices by disk usage over $args{'disk_space'} gigabytes - Time:".getEllapsed());
	@expiredIndices = findOverusageIndices($client, %args);
	indexLoop($client, 'delete_by_space', \@expiredIndices, %args);
}
# Delete by time
if(defined($args{'delete'})) {
	$logger->info("Deleting indices older than $args{'delete'} $args{'time_unit'} - Time:".getEllapsed());
	@expiredIndices = findExpiredIndices($client, 'delete', %args);
	indexLoop($client, 'delete', \@expiredIndices, %args);
}
# Close by time
if(defined($args{'close'})) {
	$logger->info("Close indices older than $args{'close'} $args{'time_unit'} - Time:".getEllapsed());
	@expiredIndices = findExpiredIndices($client, 'close', %args);
	indexLoop($client, 'close', \@expiredIndices, %args);
}
# Disable bloom filter by time
if(defined($args{'bloom'})) {
	unless($esVersion =~ /^0.9\d.9/) {
		$logger->info("Cant disable bloom filter under the 0.90.9 version of elasticsearch (elasticsearch version: $esVersion)- Time:".getEllapsed());
	} else {
		$logger->info("Disabling bloom filter indices older than $args{'bloom'} $args{'time_unit'} - Time:".getEllapsed());
		@expiredIndices = findExpiredIndices($client, 'bloom', %args);
		indexLoop($client, 'bloom', \@expiredIndices, %args);
	}
}
# Optimize index
if(defined($args{'optimize'})) {
	$logger->info("Optimize indices older than $args{'optimize'} $args{'time_unit'} - Time:".getEllapsed());
	@expiredIndices = findExpiredIndices($client, 'optimize', %args);
	indexLoop($client, 'optimize', \@expiredIndices, %args);
}
# print Dumper(\@expiredIndices);


$locker->deleteLock();

$logger->info("All DONE, FINISH... - Time:".getEllapsed());
###########################
#
# functions or subroutines
#
###########################

# Generator that yields over usage indices.

# :return: Yields tuples on the format ``(index_name, 0)`` where index_name
# is the name of the expired index. The second element is only here for
# compatiblity reasons.
sub findOverusageIndices {
	my ($client,%args) = @_;
	
    my $diskUsage = 0;
    my $diskLimit = $args{'disk_space'} * 2**30;

    my @sortedIndices = $client->indices->get_settings();
	@sortedIndices = sort(keys %{$sortedIndices[0]});

	my @return;
	foreach(@sortedIndices) {
		my $indexName = $_;
		
		my $wrongIndex = 1;
		foreach(@{$args{'prefix'}}) {
			my $prefix = $_;
			if($indexName =~ /^$prefix/) {
				$wrongIndex = 0;
				last;
			}
		}
		if($wrongIndex) {
			$logger->debug("Skipping index due to missing prefix @{$args{'prefix'}}: $indexName");
            next;
		}
		unless (indexIsClosed($client, $indexName)) {
            my $indexSize = $client->indices->status(index=>$indexName);
            $diskUsage += $indexSize->{indices}{$indexName}{index}{primary_size_in_bytes};
		} else {
			$logger->warn("Cannot check size of index $indexName because it is closed.  Size estimates will not be accurate.");
		}
		
		if ($diskUsage > $diskLimit) {
            push(@return,[$indexName, 0]);
		} else {
            $logger->info("skipping $indexName, disk usage is " . $diskUsage/2**30 . " GB and disk limit is " . $diskLimit/2**30 . " GB.");
		}
	}
	
	return @return;
}

# Generator that yields expired indices.

# :return: Yields tuples on the format ``(index_name, expired_by)`` where index_name
# is the name of the expired index and expired_by is the interval (timedelta) that the
# index was expired by.
sub findExpiredIndices {
	my ($client,$type,%args) = @_;
    
	my $utcNow = time;
	if($args{'time_unit'} eq 'hourly') {
		$utcNow = AdjustDate::adjustDate($utcNow,{'hour'=>$args{($type)}*-1});
	} else {
		$utcNow = AdjustDate::adjustDate($utcNow,{'day'=>$args{($type)}*-1});
	}
	my @sortedIndices = $client->indices->get_settings();
	@sortedIndices = sort(keys %{$sortedIndices[0]});
	my @return;
	foreach(@sortedIndices) {
		my $indexName = $_;
		my $wrongIndex = 1;
		my $prefix = '';
		foreach my $pref (@{$args{'prefix'}}) {
			if($indexName =~ /^\Q$pref\E/) {
				$prefix = $pref;
				$wrongIndex = 0;
				last;
			}
		}
		if($wrongIndex) {
			$logger->debug("Skipping index due to missing prefix [@{$args{'prefix'}}]: $indexName");
            next;
		}
		
		my @unprefixedIndexNameParts = split(/\Q$args{'separator'}\E/, substr($indexName,length($prefix)));
		
		if(($args{'time_unit'} eq 'hourly' && scalar(@unprefixedIndexNameParts)<4)
			|| ($args{'time_unit'} eq 'daily' && scalar(@unprefixedIndexNameParts)<3)
		) {
			$logger->debug("Skipping $indexName because it is of a type (hourly or daily) that I\'m not asked to evaluate.");
			next;
		}
		foreach(@unprefixedIndexNameParts) {
			$_ =~ s/[^0-9]//;
		}
		my $indexTime = Time::y2038::timelocal(0,0,(defined($unprefixedIndexNameParts[3])?int($unprefixedIndexNameParts[3]):0),int($unprefixedIndexNameParts[2]),int($unprefixedIndexNameParts[1])-1,int($unprefixedIndexNameParts[0])-1900);

		# if the index is older than the cutoff
        if ($indexTime < $utcNow) {
            push(@return,[$indexName, $utcNow-$indexTime]);
		} else {
            $logger->info("$indexName is " . ($utcNow-$indexTime) . " above the cutoff.");
		}
	}
	return @return;
}

# Return True if index is closed
sub indexIsClosed {
	my ($client,$indexName) = @_;
	my $index_metadata;
	if($esVersion =~ /^1./) {
        $index_metadata = $client->cluster->state(
            index=>$indexName,
            metric=>'metadata',
        );
	} else {
        # 0.90 params:
        $index_metadata = $client->cluster->state(
            filter_blocks=>'True',
            filter_index_templates=>'True',
            filter_indices=>$indexName,
            filter_nodes=>'True',
            filter_routing_table=>'True'
        );
	}
    return $index_metadata->{metadata}{indices}{$indexName}{state} eq 'close'?1:0;
}


# OP_MAP = {
    # 'close': (_close_index, {'op': 'close', 'verbed': 'closed', 'gerund': 'Closing'}),
    # 'delete': (_delete_index, {'op': 'delete', 'verbed': 'deleted', 'gerund': 'Deleting'}),
    # 'optimize': (_optimize_index, {'op': 'optimize', 'verbed': 'optimized', 'gerund': 'Optimizing'}),
    # 'bloom': (_bloom_index, {'op': 'disable bloom filter for', 'verbed': 'bloom filter disabled', 'gerund': 'Disabling bloom filter for'}),
# }

sub indexLoop {
	my ($client, $operation, $expiredIndices, %args) = @_;
	my %opMap = (
		'close' => {'op' => 'close', 'verbed' => 'closed', 'gerund' => 'Closing'},
		'delete' => {'op' => 'delete', 'verbed' => 'deleted', 'gerund' => 'Deleting'},
		'delete_by_space' => {'op' => 'delete', 'verbed' => 'deleted', 'gerund' => 'Deleting'},
		'optimize' => {'op' => 'optimize', 'verbed' => 'optimized', 'gerund' => 'Optimizing'},
		'bloom' => {'op' => 'disable bloom filter for', 'verbed' => 'bloom filter disabled', 'gerund' => 'Disabling bloom filter for'},
	);
    my %words = %{$opMap{($operation)}};
	foreach (@$expiredIndices) {
		my ($indexName,$expiration) = @$_;
		
		if(defined($args{'dry-run'}) && $args{'dry-run'} == 1) {
			if($operation eq 'delete_by_space') {
				$logger->info("Would have attempted  ".lc($words{'gerund'})." index $indexName due to space constraints.");
			}else {
				$logger->info("Would have attempted ".lc($words{'gerund'})." index $indexName because it is $expiration older than the calculated cutoff.");
			}
            next;
		}
		if($operation eq 'delete_by_space') {
			$logger->info("Attempting to ".lc($words{'op'})." index $indexName due to space constraints.");
		}else {
			$logger->info("Attempting to $words{'op'} index $indexName because it is $expiration older than cutoff.");
		}
		my $skipped = 1;
		if($operation eq 'delete_by_space' || $operation eq 'delete') {
			$client->indices->delete(index=>$indexName);
			$skipped = 0;
		} elsif($operation eq 'close') {
			if(indexIsClosed($client, $indexName)) {
				$logger->info("Skipping index $indexName: Already closed.");
			} else {
				$client->indices->close(index=>$indexName);
				$skipped = 0;
			}
		} elsif($operation eq 'bloom') {
			if(indexIsClosed($client, $indexName)) {
				$logger->info("Skipping index $indexName: Already closed.");
			} else {
				$client->indices->put_settings(index=>$indexName, body=>'index.codec.bloom.load=false');
				$skipped = 0;
			}
		} elsif($operation eq 'optimize') {
			if(indexIsClosed($client, $indexName)) {
				$logger->info("Skipping index $indexName: Already closed.");
			} else {
				my $shards = $client->indices->segments(index=>$indexName);
				my @shards = $shards->{indices}{$indexName}{shards};
				my $segmentcount = 0;
				foreach(@shards) {
					my $shardnum = $_;
					foreach(keys %{$shardnum}) {
						my $valami = $_;
						$segmentcount+= $shardnum->{($valami)}[0]{num_search_segments};
					}
				}
				$logger->debug("Index $indexName has ".scalar($shards)." shards and $segmentcount segments total.");
				if ($segmentcount > (scalar($shards) * $args{'max_num_segments'})) {
					$logger->info("Optimizing index $indexName to $args{'max_num_segments'} segments per shard.  Please wait...");
					$client->indices->optimize(index=>$indexName, max_num_segments=>$args{'max_num_segments'});
					$skipped = 0;
				} else {
					$logger->info("Skipping index $indexName: Already optimized.");
				}
			}
		}

        next if $skipped;

        # if no error was raised and we got here that means the operation succeeded
        $logger->info("$indexName: Successfully  $words{'verbed'}.'");
		
	}
	$logger->info(uc($words{'op'})." index operations completed.");
}

sub getEllapsed {
	return  Time::HiRes::tv_interval( $ellapsedTime );
}

sub usage {
	my $message = shift;
	if (defined $message && length $message) {
		$message .= "\n\n" unless $message =~ /\n$/;
	}

	my $command = $0;
	$command =~ s#^.*/##;

	print $message,
		"Curator for Elasticsearch indices. Can delete (by space or time), close, disable bloom filters and optimize (forceMerge) your indices.\n" .
		"usage: $command [options]\n" .
		"       -h, --host => Elasticsearch host. Default: localhost\n" .
		"       -p, --port => Elasticsearch port. Default: 9200\n" .
		"       -t, --timeout => Elasticsearch timeout. Default: 30\n" .
		"       -p, --prefix => Prefix for the indices. Indices that do not have this prefix are skipped. Default: logstash-\n" .
		"       -s, --separator => Time unit separator. Default: .\n" .
		"       -C, --curation_style => Curate indices by [time, space] Default: time\n" .
		"       -T, --time_unit => Unit of time to reckon by: [days, hours] Default: days\n" .
		"       -d, --delete => Delete indices older than n TIME_UNITs.\n" .
		"       -c, --close => Close indices older than n TIME_UNITs.\n" .
		"       -b, --bloom => Disable bloom filter for indices older than n TIME_UNITs.\n" .
		"       -g, --disk_space => Disable bloom filter for indices older than n TIME_UNITs.\n" .
		"       -m, --max_num_segments => Maximum number of segments, post-optimize. Default: 2\n" .
		"       -o, --optimize => Optimize (Lucene forceMerge) indices older than n TIME_UNITs. Must increase timeout to stay connected throughout optimize operation, recommend no less than 3600.\n" .
		"       --dry-run => If true, does not perform any changes to the Elasticsearch indices.\n";
		
	$locker->deleteLock();
	die();
}

sub validateArgs {
	my %args = %{$_[0]};
	
	
	usage('Must specify at least one of --delete, --close, --bloom, --optimize ,--disk_space!!!!') if(!defined($args{'delete'}) && !defined($args{'close'}) && !defined($args{'bloom'}) && !defined($args{'optimize'}) && !defined($args{'disk_space'}));
	
	usage('Values for --delete, --close, --bloom, --optimize must be > 0!!!!') 
		if((defined($args{'delete'}) && $args{'delete'} <1) 
			|| (defined($args{'close'}) && $args{'close'} <1) 
			|| (defined($args{'bloom'}) && $args{'bloom'} <1) 
			|| (defined($args{'optimize'}) && $args{'optimize'} <1)
			|| (defined($args{'optimize'}) && $args{'optimize'} <1));

	usage('Wrong time_unit!!!!') unless($args{'time_unit'} =~ /^days|hours$/);
	
	usage('Wrong curation_style!!!!!') unless($args{'curation_style'} =~ /^time|space$/);
	
	usage('Cannot specify --disk_space and --curation_style "time"!!!!') if(defined($args{'disk_space'}) && $args{'curation_style'} eq 'time');
	
	usage('Timeout should be much higher for optimize transactions, recommend no less than 3600 seconds!!!!') if(defined($args{'optimize'}) && $args{'timeout'} < 300);
	
	usage('Cannot specify --curation_style "space" and any of --delete, --close, --bloom, --optimize!!!!') if($args{'curation_style'} eq 'space' && (defined($args{'delete'}) || defined($args{'close'}) || defined($args{'bloom'}) || defined($args{'optimize'})));
	
	usage('Value for --disk_space must be greater than 0!!!!') if(defined($args{'disk_space'}) && $args{'disk_space'} <= 0);
}
