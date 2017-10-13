use strict;
use warnings;
use feature qw|say state|;
use utf8;
use FindBin;
use lib "$FindBin::Bin/../modules";

package main;
use Data::Dumper;
$Data::Dumper::Indent = 1;
use Getopt::Std;
use List::Util qw|pairs any all none min max|;
use URI::Escape;
use Carp;
use Cwd qw/abs_path/;
use DBI;
use Get::Hzn;
use Get::DLS;
use Utils qw/date_hzn_8601 date_8601_hzn date_unix_8601/;

use constant EXPORT_ID => date_unix_8601(time);

use constant HEADER => <<'#';
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE marc [
	<!ELEMENT collection (record*)>
	<!ATTLIST collection xmlns CDATA "">
	<!ELEMENT record (leader,controlfield+,datafield+)>
	<!ELEMENT leader (#PCDATA)>
	<!ELEMENT controlfield (#PCDATA)>
	<!ATTLIST controlfield tag CDATA "">
	<!ELEMENT datafield (subfield+)>
	<!ATTLIST datafield tag CDATA "" ind1 CDATA "" ind2 CDATA "">
	<!ELEMENT subfield (#PCDATA)>
	<!ATTLIST subfield code CDATA ""> 
]>
#
# <collection xmlns="http://www.loc.gov/MARC21/slim">
# Tind doesn't like the xmlns attribute, 
# which is ok since their marcxml doesn't conform to to it anyway  

use constant TYPE => {
	map => 'Maps',
	sp => 'Speeches',
	vot => 'Voting Data',
	img => 'Images and Sounds',
	docpub => 'Documents and Publications',
	rd => 'Resolutions and Decisions',
	rpt => 'Reports',
	mr => 'Meeting Records',
	lnv => 'Letters and Notes Verbales',
	pub => 'Publications',
	drpt => 'Draft Reports',
	drd	=> 'Draft Resolutions and Decisions',
	pr => 'Press Releases',
	ai => 'Administrative Issuances',
	ta => 'Treaties and Agreements',
	lco => 'Legal Cases and Opinions',
	nws => 'NGO Written Statements',
	pet => 'Petitions',
	cor => 'Concluding Observations and Recommendations',
	res => 'Resolutions',
	dec => 'Decisions',
	prst => 'Presidential Statements',
	sgr => 'Secretary-General\'s Reports',
	asr => 'Annual and Sessional Reports',
	per => 'Periodic Reports',
	vbtm => 'Verbatim Records',
	sum => 'Summary Records',
	sgl	=> 'Secretary-General\'s Letters',
};

use constant AUTH_TYPE => {
	100 => 'PERSONAL',
	110 => 'CORPORATE',
	111 => 'MEETING',
	130 => 'UNIFORM',
	150 => 'TOPICAL',
	151 => 'GEOGRAPHIC',
	190 => 'SYMBOL',
	191 => 'AGENDA'
};

use constant DESC => {
	'[cartographic information]' => 'a',
	'[video recording]' => 'v',
	'[sound recording]' => 's',
	'ORAL HISTORY' => 's'
};

use constant LANG_ODS_ISO => {
	A => 'AR',
	C => 'ZH',
	E => 'EN',
	F => 'FR',
	R => 'RU',
	S => 'ES',
	O => 'DE'
};

use constant LANG_ISO_STR => {
	# unicode normalization form C (NFC)
	AR => ' العربية ',
	ZH => '中文 => ',
	EN => 'English ',
	FR => 'Français',
	RU => 'Русский ',
	ES => 'Español ',
	DE => 'Deutsch',
};

use constant LANG_STR_ISO => {
	# NFC
	العربية => 'AR',
	中文 => 'ZH',
	Eng => 'EN',
	English => 'EN',
	Français => 'FR',
	Русский => 'RU',
	Español => 'ES',
	Deutsch => 'DE',
	Other => 'DE',
	
	# alt encoding normalization form? not sure how to convert
	Français => 'FR',
	Español => 'ES',
};

RUN: {
	MAIN(options());
}

sub options {
	my @opts = (
		['h' => 'help'],
		['a' => 'export auths'],
		['b' => 'export bibs'],
		['o:' => 'xml output directory'],
		['d:' => 'data store dir'],
		['m:' => 'modified since'],
		['u:' => 'modified until'],
		['s:' => 'sql criteria'],
		['S:' => 'sql script'],
		['3:' => 's3 database'],
		['e:' => 'error report']
	);
	getopts (join('',map {$_->[0]} @opts), \my %opts);
	if (! %opts || $opts{h}) {
		say join ' - ', @$_ for @opts;
		exit; 
	} else {
		$opts{a} && $opts{b} && die q{must choose only one of opts "a" or "b"}."\n";
		$opts{a} || $opts{b} || die q{boolean opt "a" or "b" required}."\n";
		$opts{m} || $opts{s} || $opts{e} || die q{opt "m" (date) or "s" (sql) required}."\n";
		$opts{m} && length $opts{m} < 8 && die qq{datetime opts "m" must be at least 8 characters"};
		$opts{3} || die q{opt "3" (s3 database path) required}."\n";
		-e $opts{3} || die qq{s3 database path is invalid}; 
		$opts{a} && ($opts{t} = 'auth');
		$opts{b} && ($opts{t} = 'bib');
	}
	return \%opts;
}

sub MAIN {
	my $opts = shift;
	
	my %dispatch = (
		#s => \&export_range,
		m => \&export_from,
		#c => \&export_by_criteria,
		#q => \&export_by_criteria,
		#x => \&thesaurus,
	);

	#update_dls_data($opts->{d}); # only used to recreate dls 856 fields. use FFT only for now
	#update_hzn_data($opts->{d}); # data used to find duplicate 035 ctrl#s
	
	run_export($opts);
}

sub run_export {
	my $opts = shift;
	
	my $ids;
	if ($opts->{m}) {
		$ids = modified_since(@{$opts}{qw/t m u/});
	} elsif ($opts->{s}) {
		$ids = get_by_sql($opts->{s});
	} elsif ($opts->{S}) {
		$ids = get_by_sql_script($opts->{s});
	} elsif ($opts->{e}) {
		use File::Slurp;
		my $errors = read_file($opts->{e});
		my @ids = $errors =~ />\(DHL\)([^<]+)/g;
		$ids = \@ids;
	}
	
	my $c = scalar @$ids;
	if ($c) {
		say "*** ok. found $c export candidates ***";	
	} else {
		say "no export candidates found" and return;
	}
	
	#my $dups = $opts->{t} eq 'bib' ? duplicate_ctrls() : undef; # ($opts->{d}) : undef; # updates hzn ctrl data store
	my $dups = {};
	my ($stime,$total,$chunks,$from) = (time,0,int(scalar(@$ids / 1000))+1,0);
	
	my $fh = init_xml($opts);
	for my $chunk (0..$chunks) {
		my $to = $from + 1000;
		my $filter = join ',', grep {defined($_)} @$ids[$from..$to];
		last unless $filter;
		say 'gathering data for chunk '.($chunk+1).'...';
		my $item = item_data($filter);
		my $audit = audit_data($opts->{t},$filter);
		#my $dls = dls_data($opts->{d},$filter); # updates dls data store
		#my $s3 = s3_data($opts->{d},$filter);
		say "writing xml...";
		$total += write_xml (
			type => $opts->{t},
			filter => $filter,
			s3_dbh => DBI->connect('dbi:SQLite:dbname='.$opts->{3},'',''),
			#dls_data => $dls,
			item => $item,
			dups => $dups,
			audit => $audit,
			output_fh => $fh,
			candidates => scalar (split ',', $filter) - 1,
		);
		$from += 1000;
	}
	cut_xml($fh);

	say "> done. wrote $total records out of $c candidates in ".(time - $stime).' seconds';
	my $outfile = abs_path($opts->{outfile}) =~ s|/|\\|gr;
	say "> output file: $outfile";
	
	open my $log,'>>','log';
	say {$log} join "\t", EXPORT_ID, $outfile;
	
	system qq{echo $outfile | clip};
	say '> the output file path is in your clipboard';
	system qq{start https://digitallibrary.un.org/batchuploader/metadata?ln=en};
}

sub init_xml {
	my $opts = shift;
	$opts->{o} ||= "$FindBin::Bin/../XML";
	my $dir = $opts->{o};
	(-e $dir || mkdir $dir) or die qq|can't make dir "$opts->{o}"|;
	my $fn;
	if ($opts->{m}) {
		$fn = "$dir/$opts->{t}\_from_$opts->{m}\-";
		if ($opts->{u}) {
			$fn .= $opts->{u};
		} else {
			$fn .= EXPORT_ID;
		}
	} elsif ($opts->{s}) {
		$fn = "$dir/".($opts->{s} =~ s/\s/_/gr);
	} elsif ($opts->{e}) {
		$fn = "$dir/".$opts->{e};
	}
	$fn .= '.xml';
	$opts->{outfile} = $fn;
	open my $fh, ">:utf8", $fn; 
	say {$fh} join "\n", HEADER, '<collection>';
	return $fh;
}

sub write_xml {
	my %p = @_; #print Dumper \%p;
	state $range_control = 0;
	my ($ctype,$count) = (ucfirst $p{type},0);
	"Get::Hzn::Dump::$ctype"->new->iterate (
		criteria => $p{filter},
		encoding => 'utf8',
		callback => sub {
			my $record = shift;
			_000($record);
			_005($record);
			_035($record,$p{type},$p{dups});
			_998($record,$p{audit}->{$record->id});
			if ($p{type} eq 'bib') {
				return unless 
					$record->has_tag('191') 
					|| $record->has_tag('791') 
					|| any {$_ eq 'DHU'} $record->get_values('099','b');
				_007($record);
				_020($record);
				_650($record);
				_856($record,$p{s3_dbh},undef); # also handles FFT
				_949($record,$p{item}->{$record->id});
				_993($record);
				_967($record);
				_996($record);
				_989($record);
			} elsif ($p{type} eq 'auth') {
				return if any {$_->xref > $record->id} @{$record->get_fields(qw/400 410 411 450 451/)}
					|| any {$_ =~ /^[PT]/} $record->get_values('035','a');
				_150($record); # also handles 450 and 550
				_4xx($record);
				_980($record);
			} else {
				die 'wtf';
			}
			_xrefs($record);
			$p{output_fh} //= *STDOUT;
			print {$p{output_fh}} $record->to_xml;
			$count++;
			say "wrote $count / ".$p{candidates};
		}
	);
	
	return $count;
}

sub cut_xml {
	my $fh = shift;
	print $fh '</collection>' if $fh;
}

sub modified_since {
	#my $opts = shift;
	#my ($type,$from,$to) = @{$opts}{qw/t m u/};
	my ($type,$from,$to) = @_;
	#$opts->{modified_type} ||= 'all';
	my $mod_type = 'all';
	$from = date_8601_hzn($from);
	my $fdate = $from->[0];
	my $ftime = $from->[1];
	my $sql = qq{select $type\# from $type\_control};
	my $new = "create_date > $fdate or (create_date = $fdate and create_time >= $ftime)";
	my $changed = "change_date > $fdate or (change_date = $fdate and change_time >= $ftime)";
	my %more = (
		all => qq{where (($new) or ($changed))},
		new => qq{where ($new)},
		changed => qq{\nwhere ($changed)},
	);
	$sql .= "\n".$more{$mod_type}."\n";
	if ($to) {
		$to = date_8601_hzn($to);
		my $tdate = $to->[0];
		my $ttime = $to->[1];
		my $new = "create_date < $tdate or (create_date = $tdate and create_time < $ttime)";
		my $changed = "change_date = null or change_date < $tdate or (change_date = $tdate and change_time < $ttime)";
		my %more = (
			all => qq{and (($new) and ($changed))},
			new => qq{and ($new)},
			changed => qq{and ($changed)}
		);
		$sql .= $more{$mod_type};
	}
	my @ids;
	Get::Hzn->new(sql => $sql)->execute (
		callback => sub {
			my $row = shift;
			my $id = shift @$row;
			push @ids, $id;
		}
	);
	return \@ids;
}

sub by_criteria {
	my $criteria = shift;
	my $get = Get::Hzn->new (
		sql => "select bib# from bib where bib# in ($criteria)"
	);
	my @ids;
	$get->execute (
		callback => sub {
			my $row = shift;
			my $id = shift @$row;
			push @ids, $id;
		}
	);
	return \@ids;
}

sub _xrefs {
	my $record = shift;
	for my $field ($record->get_fields) {
		if (my $xref = $field->xref) {
			$xref = '(DHLAUTH)'.$xref;
			$field->xref($xref);
		}
	}
}

sub _000 {
	my $record = shift;
	my $l = substr($record->leader,0,24); # chop off end of illegally long leaders in some old records
	$l =~ s/\x{1E}/|/g; # special case for one record with \x1E in leader (?)
	$record->get_field('000')->text($l);
}

sub _005 {
	my $record = shift;
	$record->delete_tag('005');
}

sub _007 {
	my $record = shift;
	for my $field ($record->get_fields(qw/191 245/)) {
		while (my ($key,$val) = each %{&DESC}) {
			if ($field->text =~ /\Q$key\E/) {
				$record->add_field(MARC::Field->new(tag => '007', text => $val));
			}
		}
	}
}

sub _020 {
	my $record = shift;
	$_->delete_subfield('c') for $record->get_fields('020');
}

sub _035 {
	my ($record,$type,$dups) = @_;
	
	for my $field ($record->get_fields('035')) {
		my $ctr = $field->get_sub('a');
		#next unless $dups->{$ctr} > 0;
		my $pre = substr $ctr,0,1;
		my $new = $record->id.'X';
		$new = $pre.$new if $pre =~ /[A-Z]/;
		$field->set_sub('a',$new,replace => 1);
		$field->set_sub('z',$ctr);
	}
	
	#$record->delete_field
	
	my $pre = $type eq 'bib' ? '(DHL)' : '(DHLAUTH)';
	my $nf = MARC::Field->new(tag => '035');
	$nf->sub('a',$pre.$record->id);
	$record->delete_tag('001');
	$record->add_field($nf);
}

sub _150 {
	my $record = shift;
	if (my $field = $record->get_field('150')) {
		if ($field->ind1 eq '9') {
			$field->change_tag('151');
			for ($record->get_fields('450')) {
				$_->change_tag('451');
			}
			for ($record->get_fields('550')) {
				$_->change_tag('551') if $_->ind1;
			}		
		}
	}
}

sub _4xx {
	my $record = shift;
	for my $tag (qw/400 410 411 430 450 490/) {
		$_->delete_subfield('0') for $record->get_fields($tag);
	}
	#$_->delete_subfield('0') for $record->get_fields(qw/400 410 411 430 450 490/);
}

sub _650 {
	my $record = shift;
	for ($record->fields('650')) {
		my $ai = $_->auth_indicators;
		$ai && (substr($ai,0,1) eq '9') && $_->change_tag('651');
	}
}

sub _856 {
	my ($record,$s3,$dls) = @_;
	
	my $bib = $record->id;
	
	my $thumb_url;
	THUMB: for my $f ($record->get_fields('856')) {
		if ($f->check('3',qr/Thumbnail/)) {
			$thumb_url = $f->get_sub('u');
		}
	}
	
	FIELDS: for my $hzn_856 ($record->fields('856')) {
		my $url = $hzn_856->get_sub('u');
		my $lang = $hzn_856->get_sub('3');
		if (index($url,'http://daccess-ods.un.org') > -1) {
			
			$record->delete_field($hzn_856);
			
			die "could not detect language for file in bib# ".$record->id if (! $lang);	
					
			S3:
			#my $key = $s3->{$record->id}->{LANG_STR_ISO->{$lang}};
			
			my $iso = LANG_STR_ISO->{$lang};
			my $sql = qq|select key from keys where bib = $bib and lang = "$iso"|;
			#say $sql;
			my $res = $s3->selectrow_arrayref($sql);
			my $key = $res->[0] if $res;
			next unless $key;
			
			my $newfn = (split /\//,$key)[-1];
			$newfn = clean_fn($newfn);
			if (! grep {$_ eq substr($newfn,-6,2)} keys %{&LANG_ISO_STR}) {
				substr($newfn,-7,2) = '-'.$iso;
			}
			
			my $FFT = MARC::Field->new(tag => 'FFT')->set_sub('a','http://undhl-dgacm.s3.amazonaws.com/'.uri_escape($key));
			$FFT->set_sub('n',$newfn);
			$FFT->set_sub('d',$lang);
			$record->add_field($FFT);
			
		} elsif (grep {$url =~ /$_/} qw|s3.amazonaws dag.un.org|) {
			
			next if $hzn_856->check('3',qr/Thumbnail/);
			
			$record->delete_field($hzn_856);
			my $newfn = (split /\//,$url)[-1];
			$newfn = clean_fn($newfn);
			
			if ($url =~ m|(https?://.*/)(.*)|) {
				$url = $1.uri_escape($2);
			} else {
				die 's3 url error';
			}
		
			my $FFT = MARC::Field->new(tag => 'FFT')->set_sub('a',$url);
			$FFT->set_sub('n',$newfn);
			$FFT->set_sub('d',$lang);
			#$FFT->set_sub('f',$hzn_856->get_sub('q'));
			$FFT->set_sub('x',$thumb_url) if $thumb_url;
			$record->add_field($FFT);
		} else {
			
		}
	}
	
	EXTRAS: {
		# in process
		last;
		my $sql = qq|select key from other where bib = $bib|;
		my $extras = $s3->selectall_arrayref($sql);
		for my $key (@$extras) {
			my $newfn = (split /\//,$key)[-1];
			my $isos = $1 if $newfn =~ /-([A-Z]+)\.\w+/;
			my @langs;
			while ($isos) {
				my $iso = substr $isos,0,2,'';
				push @langs, LANG_ISO_STR->{$iso};
			}
			my $FFT = MARC::Field->new(tag => 'FFT')->set_sub('a','http://undhl-dgacm.s3.amazonaws.com/'.uri_escape($key));
			$FFT->set_sub('n',clean_fn($newfn));
			$FFT->set_sub('d',join(',',@langs));
			$record->add_field($FFT);
		}
	}
	
}

sub clean_fn {
	# scrub illegal characters for saving on Invenio's filesystem
	my $fn = shift;
	my @s = split '\.', $fn;
	$fn = join '-', @s[0..$#s-1];
	my $ext = $s[-1];
	$fn =~ s/\s//g;
	$fn =~ tr/[];/^^&/;
	$fn .= ".$ext";
	return $fn;
}

sub _949 {
	my ($record,$data) = @_;
	$record->delete_tag('949');
	for (keys %$data) {
		$_ eq 'places' && next;
		my $vals = $data->{$_};
		my $field = MARC::Field->new(tag => '949');
		$_ =~ s/[\x{1}-\x{1F}]//g for @$vals;
		$field->set_sub($_,shift(@$vals)) for qw/9 b i k c l z m d/;
		$record->add_field($field);
	}
}

sub _967 {
	my $record = shift;
	for my $field ($record->get_fields(qw/968 969/)) {
		$field->change_tag('967');
	}
}

sub _980 {
	my $record = shift;
	$record->add_field(MARC::Field->new(tag => '980')->set_sub('a','AUTHORITY'));
	for (keys %{&AUTH_TYPE}) {
		if ($record->has_tag($_)) {
			$record->add_field(MARC::Field->new(tag => '980')->set_sub('a',AUTH_TYPE->{$_}));
			last;
		}
	}
}

sub _make_989 {
	my $field = MARC::Field->new(tag => '989');
	$field->set_sub($_->key,TYPE->{$_->value}) for pairs @_;
	return $field;
}

sub _989 {
	my $r = shift;
	my $make = \&_make_989;
			
	Q_1: {
		last unless $r->check('245','*','*[cartographic material]*')
			|| $r->check('007','*','a')
			|| $r->check('089','b','B28')
			|| $r->check('191','b','ST/LEG/UNTS/Map*');
		$r->add_field($make->(a => 'map'));
	}
	Q_2: {
		last unless $r->check('089','b','B22');
		$r->add_field($make->(a => 'sp'));
	}
	Q_3: {
		last unless $r->check('089','b','B23');
		$r->add_field($make->(a => 'vot'));
	}
	Q_4: {
		last unless $r->check('245','*',qr/(video|sound) recording/)
			|| $r->check('007','*','s')
			|| $r->check('007','*','v')
			|| $r->check('191','*','*ORAL HISTORY*');
		$r->add_field($make->(a => 'img'));
	}
	Q_5: {
		last unless $r->check('191','*','*/RES/*');
		$r->add_field($make->(a => 'docpub', b => 'rd', c => 'res'));
	}
	Q_6: {
		last unless $r->check('191','a','*/DEC/*')
			&& $r->check('089','b','B01');
		$r->add_field($make->(a => 'docpub', b => 'rd', c => 'dec'));
	}
	Q_7: {
		last unless $r->check('191','a','*/PRST/*')
			|| $r->check('089','b','B17');
		$r->add_field($make->(a => 'docpub', b => 'rd', c => 'prst'));
	}
	Q_8: {
		last unless $r->check('089','b','B01')
			&& ! $r->check('989','b',TYPE->{rd});
		$r->add_field($make->(a => 'docpub', b => 'rd'));
	}
	Q_9: {
		last unless $r->check('089','b','B15')
			&& $r->check('089','b','B16')
			&& ! $r->check('245','*','*letter*from the Secretary-General*');
		$r->add_field($make->(a => 'docpub', b => 'rd', c => 'sgr'));
	}
	Q_10: {
		last unless $r->check('089','b','B04');
		$r->add_field($make->(a => 'docpub', b => 'rd', c => 'asr'));
	}
	Q_11: {
		last unless $r->check('089','b','B14')
			&& ! $r->check('089','b','B04');
		$r->add_field($make->(a => 'docpub', b => 'rd', c => 'per'));
	}
	Q_12: {
		last unless $r->check('089','b','B16')
			&& $r->check('245','*','*Report*')
			&& $r->check('989','b','Reports');
		$r->add_field($make->(a => 'docpub', b => 'rpt'));
	}
	Q_13: {
		last unless $r->check('191','a','*/PV.*');
		$r->add_field($make->(a => 'docpub', b => 'mr', c => 'vbtm'));
	}
	Q_14: {
		last unless $r->check('191','a','*/SR.*');
		$r->add_field($make->(a => 'docpub', b => 'mr', c => 'sum'));		
	}
	Q_15: {
		last unless $r->check('089','b','B03')
			&& ! $r->check('989','b','Meeting Records');
		$r->add_field($make->(a => 'docpub', b => 'mr'));
	}
	Q_16: {
		last unless $r->check('089','b','B15')
			&& ! $r->check('245','*','Report*')
			&& ! $r->check('989','c','Secretary-General\'s*');
		$r->add_field($make->(a => 'docpub', b => 'lnv', c => 'sgl'));
	}
	Q_17: {
		last unless $r->check('089','b','B18')
			&& ! $r->check('089','b','Letters*');
		$r->add_field($make->(a => 'docpub', b => 'lnv'));
	}
	Q_18: {
		last unless $r->has_tag('022')
			|| $r->has_tag('020')
			|| $r->check('089','b','B13')
			|| $r->has_tag('079');
		$r->add_field($make->(a => 'docpub', b => 'pub'));
	}
	Q_19: {
		last unless $r->check('089','b','B08');
		$r->add_field($make->(a => 'docpub', b => 'drpt'));
	}
	Q_20: {
		last unless $r->check('089','b','B02');
		$r->add_field($make->(a => 'docpub', b => 'drd'));
	}
	Q_21: {
		last unless $r->check('191','a','*/PRESS/*')
			|| $r->check('089','b','B20');
		$r->add_field($make->(a => 'docpub', b => 'pr'));
	}	
	Q_22: {
		last unless $r->check('089','b','B12')
			|| $r->check('191','a',qr/\/(SGB|AI|IC|AFS)\//);
		$r->add_field($make->(a => 'docpub', b => 'ai'));
	}
	Q_23: {
		last unless $r->check('089','b','A19');
		$r->add_field($make->(a => 'docpub', b => 'ta'));
	}
	Q_24: {
		last unless $r->check('089','b','A15')
			|| $r->check('089','b','B25');
		$r->add_field($make->(a => 'docpub', b => 'lco'));
	}
	Q_25: {
		last unless $r->check('089','b','B21')
			|| $r->check('191','a','*/NGO/*');
		$r->add_field($make->(a => 'docpub', b => 'nws'));
	}
	Q_26: {
		last unless $r->check('191','a','*/PET/*');
		$r->add_field($make->(a => 'docpub', b => 'pet'));
	}	
	Q_27: {
		last unless $r->check('089','b','B24');
		$r->add_field($make->(a => 'docpub', b => 'cor'));
	}	
	Q_28: {
		last unless ! $r->has_tag('989');
		$r->add_field($make->(a => 'docpub'));
	}
}

sub _993 {
	my $record = shift;
	
	PRSTS: {
		my %prsts;
		for ($record->fields('991')) {
			if (my $text = $_->get_sub('e')) {
				if ($text =~ /.*?(S\/PRST\/[0-9]+\/[0-9]+)/) {
					$prsts{$1} = 1;
				}
			}
		}
		for (keys %prsts) {
			my $field = MARC::Field->new;
			$field->tag('993')->inds('5')->sub('a',$_);
			$record->add_field($field);
		}
	}
	SPLIT: {
		for my $field ($record->fields('993')) {
			if (my $text = $field->sub('a')) {
				my @syms = split_993($text);
				my $inds;
				if ($syms[0]) {
					$inds = $field->inds;
					$field->ind1('9');
				} 
				for (@syms) {
					my $newfield = MARC::Field->new (
						tag => '993',
						indicators => $inds,
					);
					$newfield->sub('a',$_);
					$record->add_field($newfield);
				}
			}
		}
	}
}

sub split_993 {
	my $text = shift;
	
	return unless $text && $text =~ /([&;,]|and)/i;
	
	$text =~ s/^\s+|\s+$//;
	$text =~ s/ {2,}/ /g;
	my @parts = split m/\s?[,;&]\s?|\s?and\s?/i, $text;
	s/\s?Amended by //i for @parts;
	my $last_full_sym;
	my @syms;
	for (0..$#parts) {
		my $part = $parts[$_];
		$last_full_sym = $part if $part =~ /^[AES]\//;
		if ($part !~ /\//) {
			$part =~ s/ //g;
			if ($part =~ /^(Add|Corr|Rev)[ls]?\.(\d+)$/i) {
				push @syms, $last_full_sym.'/'.$1.".$2";
			} elsif ($part =~ /(.*)\.(\d)\-(\d)/) {
				my ($type,$start,$end) = ($1,$2,$3);
				push @syms, $last_full_sym.'/'.$type.".$_" for $start..$end;
			} elsif ($part =~ /^(\d+)$/) {
				my $type = $1 if $syms[$_-1] =~ /(Add|Corr|Rev)\.\d+$/i;
				push @syms, $last_full_sym.'/'.$type.".$_";
			} 
		} elsif ($part =~ /\//) {
			if ($part =~ /((Add|Corr|Rev)\.[\d]+\/)/i) {
				my $rep = $1;
				$part =~ s/$rep//;
				push @syms, $last_full_sym.'/'.$part;
			} elsif ($part =~ /^[AES]\//) {
				push @syms, $part;
			} 
		}
	}
	
	return @syms;
}

sub _996 {
	my $record = shift;
	if (my $field = $record->get_field('996')) {
		if (my $pv = pv_from_996($record)) {
			my $newfield = MARC::Field->new (
				tag => '993',
				indicators => '4'
			);
			$newfield->sub('a',$pv);
			$record->add_field($newfield);
		}
	}
}

sub pv_from_996 {
	my $record = shift;
	my ($symfield,$body,$session);
	my $text = $record->get_field('996')->get_sub('a');
	my $meeting = $1 if $text =~ /(\d+).. (plenary )?meeting/i;
	return if ! $meeting;
	
	for (qw/191 791/) {
		if ($symfield = $record->get_field($_)) {
			return if index($symfield->get_sub('a'),'CONF') > -1;
			$body = $symfield->get_sub('b');
			if ($session = $symfield->get_sub('c')) {
				$session =~ s/\/$//;
			}
		} else {
			next;
		}
	}
	
	say $record->id.' 996 could not detect session' and return if ! $session;
	say $record->id.' 996 could not detect body' and return if ! $body;			
	
	return if ! grep {$body eq $_} qw|A/HRC/ A/ S/|;
	
	my $pv;
	if (substr($session,-4) eq 'emsp') {
		my $num = substr($session,0,-4);
		$session = 'ES-'.$num;
		if ($num > 7) {
			$pv = $body.$session.'/PV.'.$meeting;
		} else {
			$pv = $body.'PV.'.$meeting;
		}
	} elsif (substr($session,-2) eq 'sp') {
		my $num = substr($session,0,-2);
		$session = 'S-'.$num;
		if ($num > 5) {
			$pv = $body.$session.'/PV.'.$meeting;
		} else {
			$pv = $body.'PV.'.$meeting;
		}
	} elsif ((substr($body,0,1) eq 'A') and ($session > 30)) {
		$pv = $body.$session.'/PV.'.$meeting;
	} else {
		$pv = $body.'PV.'.$meeting;
	}
	
	return $pv;	
}

sub _998 {
	my ($record,$data) = @_;
	confess $record->id if ! $data;
	my ($cr_date,$cr_time,$cr_user,$ch_date,$ch_time,$ch_user) = @$data;
	$_ ||= '' for $cr_date,$cr_time,$cr_user,$ch_date,$ch_time,$ch_user;
	my %data = ('a' => date_hzn_8601($cr_date,$cr_time),'b' => $cr_user,'c' => date_hzn_8601($ch_date,$ch_time),'d' => $ch_user);
	my $_998 = MARC::Field->new(tag => '998');
	$_998->sub($_,$data{$_}) for grep {$data{$_}} sort keys %data;
	$_998->sub('z',EXPORT_ID);
	$record->add_field($_998);
}

sub item_data {
	my ($filter) = @_;
	my %data;
	my $get = Get::Hzn->new (
		sql => qq {
			select 
				bib#,
				call_reconstructed,
				str_replace(ibarcode,char(9),"") as barcode,
				item#,
				collection,
				copy_reconstructed,
				location,
				item_status,
				itype,
				creation_date 
			from 
				item 
			where 
				bib# in ($filter)
		}
	);
	$get->execute (
		callback => sub {
			my $row = shift;
			my $bib = shift @$row;
			$row->[-1] = date_hzn_8601($row->[-1]) if $row->[-1];
			$data{$bib}{places}++; 
			my $place = $data{$bib}{places};
			$data{$bib}{$place} = $row;
		}
	);
	return \%data;
}

sub audit_data {
	my ($type,$filter) = @_;
	my %data;
	my $get = Get::Hzn->new (
		sql => qq {
			select 
				$type\#,
				create_date,
				create_time,
				create_user,
				change_date,
				change_time,
				change_user 
			from 
				$type\_control
			where 
				$type\# in ($filter)
		}
	);
	$get->execute (
		callback => sub {
			my $row = shift;
			my $id = shift @$row;
			$data{$id} = $row;
		}
	);
	return \%data;
}

sub duplicate_ctrls {
	
	say "indexing hzn 035...";
	my $get = Get::Hzn->new(sql => q|select text from z035|);
	my %seen;
	$get->execute (
		callback => sub {
			my $row = shift;
			for ($get->get_sub($row->[0],'a')) {
				$seen{$_}++;
			}
		}
	);
	
	my %return;
	while (my ($key,$val) = each %seen) {
		$return{$key}++ if $val > 1;
	}
	die "\tfailed\n" unless scalar keys %return;
	
	return \%return;
}

sub _dls_query_str {
	my $since = shift;
	$since = date_unix_8601($since);
	my $now = date_unix_8601(time);

	my ($syear,$smon,$sday) = ($1,$2,$3) if $since =~ /^(....)(..)(..)/;
	my ($nyear,$nmon,$nday) = ($1,$2,$3) if $now =~ /^(....)(..)(..)/;
	
	my @return;
	if ($syear eq $nyear && $smon eq $nmon) {
		my $days = join '|', map {sprintf '%02d', $_} $sday..$nday;
		push @return, "$nyear$nmon($days)";
	} elsif ($syear eq $nyear) {
		my $days = join '|', map {sprintf '%02d', $_} $sday..31;
		push @return, "$nyear$smon($days)";
		my $months = join '|', map {sprintf '%02d', $_} $smon+1..$nmon;
		push @return, "$nyear($months)";
	} else {
		say "?";
		die "updating DLS for time spans over multiple years not supported yet ¯\_(ツ)_/¯\n";
	}
	
	return join ' OR ', map {"005:/^$_/"} @return;
}

sub range {
	my ($bib,$inc) = @_;
	return "1-${inc}" if $bib < $inc;
	my $lo = int ($bib / $inc); # - ($bib % $inc));
	$lo *= $inc;
	#my $hi = $lo + $inc -1; :\
	my $hi = $lo + $inc;
	my $range = join '-', $lo,$hi;
	return $range;
}

sub s3_data {
	my $range = shift;
	my $return;
	my $cmd = qq|aws s3 ls s3://undhl-dgacm/Drop/docs_new/$range/ --recursive|;
	#say qq|running "$cmd"...|;
	my $qx = qx|$cmd|;
	#die "s3 read error $?" unless any {$? == $_} 0, 256;
	while (none {$? == $_} 0, 256) {
		state $retry = 1;
		die "retries failed" if $retry == 5;
		say "s3 read error. retrying. $?";
		$qx = qx|$cmd|;
		$retry++;
	}
	for (split "\n", $qx) {
		my $path = substr $_, 31;
		my $bib = (split /\//, $path)[3];
		my $lang = substr $path,-6,2;
		$return->{$bib}->{$lang} = $path;
	}
	return $return;
}

sub get_by_sql {
	my $sql = shift;
	my @ids = map {$_->[0]} Get::Hzn->new(sql => $sql)->execute;
	return \@ids;
}

sub get_by_sql_script {
	my $script = shift;
	my @ids = map {$_->[0]} Get::Hzn->new(script => $script)->execute;
	return \@ids;
}

__END__