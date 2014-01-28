##
#	Title:	SSH
#	Provides subroutines to login to the server via ssh and run command
#	$Id$
#
package Locker;

use warnings;
use strict;
use Log::Log4perl qw(:easy);


# Class constructor
sub new {
	my ($lockFile) = @_;
	my $self = {};
	$self = {
	   logger		=> undef,
	   lockFile		=> $lockFile,
	   lockHandler	=> undef
	  };
	bless ($self, 'Locker');
	if(!(Log::Log4perl->initialized())) {
		Log::Log4perl->easy_init($ERROR);
	}
	$self->logger(Log::Log4perl->get_logger("Locker"));
	$self->{logger}->info("Created object of class Locker");
	return $self;
}

# createAndCheckLock($lockFile)
#
# Creates a lock file if the lock file doesn't exist
# If lock file exists but the pid listed in it does not exists
# it creates a lock file
# In any other case it dies
#
# Input:
# - The lockFile
# Output:
# - The file handle opened
sub createAndCheckLock {
	my $self = shift;
	if (-e $self->{lockFile}) {
		# The file exists we need to check now if the pid is running
		open my $lock_fh, '<', $self->{lockFile}
			or $self->{logger}->error_die("can't open lock file $self->{lockFile} after checking that it exists???: $!");
		my $pid = <$lock_fh>;
		chomp $pid;
		close $lock_fh;

		if ($self->pidIsRunning($pid)) {
			$self->{logger}->error_die("A previous logparser is running, check pid $pid (got the pid from $self->{lockFile}): $!");
		} else {
			unlink $self->{lockFile}
				or $self->{logger}->error_die("can't delete lock file $self->{lockFile} after checking that the PID $pid is not running: $!");
			open($self->{lockHandler}, "> $self->{lockFile}")
				or $self->{logger}->error_die("can't open lock file $self->{lockFile} after checking that it exists???: $!");
		}
	} else {
	open($self->{lockHandler}, "> $self->{lockFile}")
				or $self->{logger}->error_die("can't open lock file $self->{lockFile} after checking that it exists???: $!");
	}
	my $file = $self->{lockHandler};
	$file->autoflush;
	print $file  $$."\n";

	return 1;
}

# pidIsRunning
# Checks if the given pid is running
# Input:
# - The pid
# Output:
# - True if the pid is running and false otherwise
sub pidIsRunning {
	use Proc::ProcessTable;
	my ($self,$pid) = @_;
	my $table = Proc::ProcessTable->new()->table;
	my %processes = map { $_->pid => $_ } @$table;
	return exists $processes{$pid};
}

# deleteLock($fileHandler, $lockFile)
#
# closes the filehandle and deletes the lockFile;
#
# Input:
# - The open file handler
# - The lockFile
# Output:
# - None
sub deleteLock {
	my $self = shift;
	close $self->{lockHandler}
		or $self->{logger}->error("Error closing filehandler for $self->{lockFile}:$!");
	unlink $self->{lockFile}
		or $self->{logger}->error("Unable to delete the file $self->{lockFile}: $!");
}

# cleanupAndExit($message)
#
# invokes $logger->error_die but it checks before if the lock file
# exits to delete it
# Input:
# - message to send to the log
# Implicitly it also uses the global variables lockHandle and lockFile
# Output:
# None, the progam exits
sub cleanupAndExit {
	my ($self,$message) = @_;
	$self->deleteLock($self->{lockHandler}, $self->{lockFile});
	$self->{logger}->error_die($message);
}

sub logger {
  my $self = shift;
  $self->{logger} = shift if (@_);
  return $self->{logger};
}

sub lockFile {
  my $self = shift;
  $self->{lockFile} = shift if (@_);
  return $self->{lockFile};
}

sub lockHandler {
  my $self = shift;
  $self->{lockHandler} = shift if (@_);
  return $self->{lockHandler};
}

1;