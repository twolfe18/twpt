
import re, os, datetime, math, subprocess, time

def bool2str(boolean):
	if boolean: return 'y'
	else: return 'n'

def str2bool(string):
	if string == 'y': return True
	if string == 'n': return False
	raise Exception('bad string: ' + string)

def timestamp():
	import datetime
	t = datetime.datetime.now()
	# corresponds to `date +"%Y-%m-%d %H:%M:%S"`
	return t.strftime("%Y-%m-%d %H:%M:%S")

def parsetime(s):
	return time.mktime(time.strptime(s, '%Y-%m-%d %H:%M:%S'))

def all_jars_in(directory):
	def helper(d):
		if not os.path.isdir(d):
			raise exception(d + ' is not a directory!')
		cp = []
		for f in os.listdir(d):
			f = os.path.join(d, f)
			if os.path.isfile(f) and f.endswith('.jar'):
				cp.append(f)
			elif os.path.isdir(f):
				helper(f)
		return cp
	return helper(directory)

def grep(pattern, file, pcre=True):
	assert os.path.exists(file) and os.path.isfile(file)
	cmd = 'grep'
	if pcre:
		cmd += ' -P'
	cmd += ' \"'+pattern+'\"'
	cmd += ' \"'+file+'\"'
	cmd += '; exit 0'	# grep returns non-zero if no lines match!
	lines = subprocess.check_output(cmd, shell=True).strip()
	if lines:
		return lines.split('\n')
	else:
		return []

def canonicalMemoryDescription(mem, maxMB=80*1024, defaultUnit='G'):
	'''
	convert to megabytes and round up
	throw exception if bigger than maxMB
	'''
	if defaultUnit != 'G':
		raise Exception('i havent implemented this yet... yes im that lazy')
	mb = maxMB + 1
	if type(mem) is str:
		m = re.match('(\d+)(K|M|G)', mem, re.IGNORECASE)
		if m:
			mb = int(m.group(1))
			if m.group(2).upper() == 'K':
				mb = min(64, int(math.ceil(mb / 1024.0)))
			if m.group(2).upper() == 'G':
				mb = mb * 1024
		else: raise Exception('unknown format: ' + mem)
	elif type(mem) is int:
		mb = mem * 1024
	elif type(mem) is float:
		mb = int(math.ceil(mem * 1024.0))
	else:
		raise Exception('what is this? ' + mem)
	if mb > maxMB:
		raise Exception('you are asking for too much memory! ' + str(mb) + 'MB')
	return str(mb) + 'M'


