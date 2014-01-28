##
#	Title:	SSH
#	Provides subroutines to login to the server via ssh and run command
#	$Id$
#
package AdjustDate;

use Time::y2038;
use warnings;
use strict;
##
# Return the days in the month
#
# Parameters:
# 		int		- timestamp
# 		hash	- adjust parameters e.g. month => 6, year => 1
# 
# Returns
#     int 	- days
##
sub adjustDate {
    my $time = shift;
	my $args = shift;
    my ($element, $dim, $self);
    my $fix_month = 0;
	
	my @YMD             = qw( year month day );
	my @HMS             = qw( hour minute second );
	my @YMDHMS          = (@YMD, @HMS);

	@$self{ @YMDHMS } = reverse( ( localtime($time) )[0..5] );
    # If we're only adjusting by a month or a year, then we fix the day 
    # within the range of the number of days in the new month.  For example:
    # 2007-01-31 + 1 month = 2007-02-28.  We must handle this for a year
    # adjustment for the case: 2008-02-29 + 1 year = 2009-02-28
    if ((scalar(keys %$args) == 1) &&
        (defined $args->{ month } || defined $args->{ months } ||
         defined $args->{ year }  || defined $args->{ years })) {
        $fix_month = 1;
    }
    
    # allow each element to be singular or plural: day/days, etc.
    foreach $element (@YMDHMS) {
        $args->{ $element } = $args->{ "${element}s" }
            unless defined $args->{ $element };
    }

    # adjust the time by the parameters specified
    foreach $element (@YMDHMS) {
        $self->{ $element } += $args->{ $element }
            if defined $args->{ $element };
    }

    # Handle negative seconds/minutes/hours
    while ($self->{ second } < 0) {
        $self->{ second } += 60;
        $self->{ minute }--;
    }
    while ($self->{ minute } < 0) {
        $self->{ minute } += 60;
        $self->{ hour   }--;
    }
    while ($self->{ hour } < 0) {
        $self->{ hour   } += 24;
        $self->{ day    }--;
    }

    # now positive seconds/minutes/hours
    if ($self->{ second } > 59) {
        $self->{ minute } += int($self->{ second } / 60);
        $self->{ second } %= 60;
    }
    if ($self->{ minute } > 59) {
        $self->{ hour   } += int($self->{ minute } / 60);
        $self->{ minute } %= 60;
    }
    if ($self->{ hour   } > 23) {
        $self->{ day    } += int($self->{ hour } / 24);
        $self->{ hour   } %= 24;
    }

    # Handle negative days/months/years
    while ($self->{ day } <= 0) {
        $self->{ month }--;
        unless ($self->{ month } > 0) {
            $self->{ month } += 12;
            $self->{ year  }--;
        }
        $self->{ day } += AdjustDate::days_in_month($self->{ year },$self->{ month });
    }
    while ($self->{ month } <= 0) {
        $self->{ month } += 12;
        $self->{ year } --;
    }
    while ($self->{ month } > 12) {
        $self->{ month } -= 12;
        $self->{ year  } ++;
    }
	
    # handle day wrap-around
    while ($self->{ day } > ($dim = AdjustDate::days_in_month($self->{ year },$self->{ month }))) {
        # If we're adjusting by a single month or year and the day is 
        # greater than the number days in the new month, then we adjust
        # the new day to be the last day in the month.  Otherwise we 
        # increment the month and remove the number of days in the current
        # month. 
        if ($fix_month) {
            $self->{ day } = $dim;
        } 
        else {
            $self->{ day } -= $dim;
            if ($self->{ month } == 12) {
                $self->{ month } = 1;
                $self->{ year  }++;
            }
            else {
                $self->{ month }++;
            }
        }
    }
	
    return Time::y2038::timelocal($self->{ second },$self->{ minute },$self->{ hour },$self->{ day },$self->{ month },$self->{ year });
}


##
# Return the days in the month
#
# Parameters:
# 		int	- year
# 		int	- month
# 
# Returns
#     int 	- days
##
sub days_in_month {
    my $year  = shift;
    my $month = shift;
    if ($month == 3 || $month == 5 || $month == 8 || $month == 10) {
        return 30;
    }
    elsif ($month == 1) {
        return AdjustDate::leap_year($year) ? 29 : 28;
    }
    else {
        return 31;
    }
}

##
# Is it leap year?
#
# Parameters:
# 		int	- year
# 
# Returns
#     boolean 	- 
##
sub leap_year {
    my $year = shift;
    if ($year % 4) {
        return 0;
    }
    elsif ($year % 400 == 0) {
        return 1;
    }
    elsif ($year % 100 == 0) {
        return 0;
    }
    else {
        return 1;
    }
}

1;