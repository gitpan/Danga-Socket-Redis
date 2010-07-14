package Danga::Socket::Redis;
use strict;
use IO::Socket;
use Danga::Socket::Callback;
use Data::Dumper;

=head1 NAME

Danga::Socket::Redis - An asynchronous redis client.

=head1 SYNOPSIS

  use Danga::Socket::Redis;

  my $rs = Danga::Socket::Redis->new ( host => 'host',
                                       connected => \&redis_connected );

  sub redis_connected {
     $rs->set ( "key", "value" ):
     $rs->get ( "key", sub { my ( $value ) = @_; print "$key = $value\n" } );
     $rs->publish ( "newsfeed", "Twitter is down" );
     $rs->hset ( "key", "field", "value" );
     $rs->hget ( "key", "field", sub { my ( $value ) = @_ };
     $rs->subscribe ( "newsfeed", sub { my ( $chan, $msg ) = @_ } );
  }

  Danga::Socket->EventLoop;


=head1 DESCRIPTION

An asynchronous client for the key/value store redis. Asynchronous
basically means a method does not block. A supplied callback will be
called with the results when they are ready.

=head1 USAGE



=head1 BUGS

Only started, a lot of redis functions need to be added.


=head1 SUPPORT

dm @martinredmond
martin @ tinychat.com

=head1 AUTHOR

    Martin Redmond
    CPAN ID: REDS
    Tinychat.com
    @martinredmond
    http://Tinychat.com/about.php

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.


=head1 SEE ALSO

perl(1).

=cut


BEGIN {
    use Exporter ();
    use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
    $VERSION     = '0.02';
    @ISA         = qw(Exporter);
    @EXPORT      = qw();
    @EXPORT_OK   = qw(set get 
		      hset hget
		      publish subscribe);
    %EXPORT_TAGS = ();
}

our $AUTOLOAD;

our %cmds = (
	     'ping', [],
	     'exists', [ 'arg', 'callback' ],
	     'set', [ 'arg', 'arg', 'callback' ],
	     'get', [ 'arg', 'callback' ],
	     'del', [ 'arg', 'callback' ],

	     'type', [ 'arg', 'callback' ],
	     'keys', [ 'arg', 'callback' ],
	     
	     'hset', [ 'arg', 'arg', 'arg', 'callback' ],
	     'hget', [ 'arg', 'arg', 'callback' ],

	     'publish', ['arg', 'arg', 'callback'],
	     'subscribe', [ 'arg', 'callback'],
	    );

1;

sub new {
  my ($class, %args) = @_;
  my $self = bless ({}, ref ($class) || $class);
  my $peeraddr = "localhost:6379";
  $peeraddr = "$args{host}:6379" if $args{host};
  $peeraddr = "localhost:$args{port}" if $args{port};
  $peeraddr = "$args{host}:$args{port}" if $args{host} && $args{port};
  my $sock = IO::Socket::INET->new (
				    PeerAddr => $peeraddr,
				    Blocking => 0,
				   );
  $self->{connected_cb} = $args{connected} if $args{connected};
  my $a = '';
  $self->{rs} = Danga::Socket::Callback->new
    (
     handle => $sock,
     context => { buf => \$a, rs => $self },
     on_read_ready => sub {
       my $self = shift;
       my $bref = $self->read ( 1024 * 8 );
       my $buf = $self->{context}->{buf};
       if ( $bref ) {
	 $buf = length ( $$buf ) > 0 ? 
	   \ ($$buf . $$bref) :
	     $bref;
	 $self->{context}->{buf} = $self->{context}->{rs}->do_buf ( $buf );
       } else {
	 $self->close ( 'read' );
	 die "reading from redis";
       }
     },
     on_write_ready => sub {
       my $self = shift;
       $self->watch_write ( 0 );
       my $cb = delete $self->{context}->{rs}->{connected_cb};
       &$cb ( $self->{context}->{rs} ) if $cb;
     }
    );
  return bless $self;
}

sub do_buf {
  my ( $self, $buf ) = @_;
  my $o;
  while ( 1 ) {
    ( $buf, $o ) =
      $self->redis_read ( $buf );
    last unless $o;
    $self->redis_process ( $o );
  }
  return $buf;  # there may be some stuff left over from this read
}

sub redis_read {
  my ( $self, $bref ) = @_;
  return ( $bref, undef ) if length ( $$bref ) == 0;
  my $nlpos = index ( $$bref, "\n" );
  return ( $bref, undef ) if $nlpos == -1;
  my $tok = substr ( $$bref, 0, 1 );
  if ( $tok eq ':' ) {
    my $n = substr ( $$bref, 1, $nlpos - 2 );
    my $r = substr ( $$bref, $nlpos + 1 );
    return ( \$r, { type => 'int', value => $n } );
  } elsif ( $tok eq '-' ) {
    my $e = substr ( $$bref, 1, $nlpos - 2 );
    my $r = substr ( $$bref, $nlpos + 1 );
    return ( \$r, { type => 'error', value => $e } );
  } elsif ( $tok eq '+' ) {
    my $l = substr ( $$bref, 1, $nlpos - 2 );
    my $r = substr ( $$bref, $nlpos + 1 );
    return ( \$r, { type => 'line', value => $l } );
  } elsif ( $tok eq '$' ) {
    my $l = substr ( $$bref, 1, $nlpos - 2 );
    if ( $l == -1 ) {
      my $r = substr ( $$bref, $nlpos + 1 );
      return ( \$r, { type => 'bulkerror' } );
    }
    #	    warn "better check this" if length ( $$bref ) < $nlpos + 1 + $l + 2;
    return ( $bref, undef ) if length ( $$bref ) < $nlpos + 1 + $l + 2;  # need more data
    my $v = substr ( $$bref, $nlpos + 1, $l );
    my $r = substr ( $$bref, $nlpos + $l + 1 + 2 );
    return ( \$r, { type => 'bulk', value => $v } );
  } elsif ( $tok eq '*' ) {
    my $l = substr ( $$bref, 1, $nlpos - 2 );
    if ( $l == -1 ) {
      my $r = substr ( $$bref, $nlpos + 1 );
      return ( \$r, { type => 'multibulkerror' } );
    }
    my $obref = $bref;
    my $r = substr ( $$bref, $nlpos + 1 );
    $bref = \$r;
    my @res;
    while ( $l-- ) {
      my $o;
      ( $bref, $o ) = $self->redis_read ( $bref );
      return $obref unless $o;    # read more?
      push @res, $o;
    }
    return ( $bref, { type => 'bulkmulti', values => \@res } );
  } else {
    die "Danga::Socket::Redis bref", $$bref;
  }
}

sub redis_process {
    my ( $self, $o ) = @_;
    my $v = $o->{values};
    if ( $v && $v->[0]->{value} eq 'message' ) {
      if ( my $cb = $self->{subscribe}->{callback}->{$v->[1]->{value}} ) {
	&$cb ( $v->[1]->{value}, $v->[2]->{value} );
      }
      return;
    }
    my $cmd = shift @{$self->{cmdqueue}};
    if ( my $cb = $cmd->{callback} ) {
	if ( $o->{type} eq 'bulkerror' ) {
	    &$cb ( undef );
	} else {
	    &$cb ( $o->{value} );
	}
    }
}

sub DESTROY {}

sub AUTOLOAD {
  my $self = shift;
  my $cc = $AUTOLOAD;
  $cc =~ s/.*:://;
  if ( my $args = $Danga::Socket::Redis::cmds{$cc} ) {
    my $cmd = { type => $cc };
    foreach ( @{$Danga::Socket::Redis::cmds{$cc}} ) {
      if ( $_ eq 'arg' ) {
	push @{$cmd->{args}}, shift;
      } else {
	$cmd->{$_} = shift;
      }
    }
    if ( $cc eq 'subscribe' && $cmd->{callback} && $cmd->{args} ) {
      $self->{subscribe}->{callback}->{$cmd->{args}->[0]} = $cmd->{callback};
    }
    $self->redis_send ( $cmd );
  }
}

sub redis_send {
  my ( $self, $cmd ) = @_;
  $cmd->{args} = [] if $cmd->{type} eq 'ping';
  unless ( $cmd->{type} eq 'subscribe' ) {
    push @{$self->{cmdqueue}}, $cmd;
  }
  my $send = "*" . ( scalar ( @{$cmd->{args}} ) + 1 ) . "\r\n" .
    "\$" . length ( $cmd->{type} ) . "\r\n" .
      $cmd->{type} . "\r\n";
  foreach ( @{$cmd->{args}} ) {
    $send .= "\$" . length ($_) . "\r\n$_\r\n";
  }
  $self->{rs}->write ( $send );
}
