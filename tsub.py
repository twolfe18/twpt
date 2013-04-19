#!/usr/bin/env python

import os, codecs, subprocess, time
from FileDict import FileDict
from Misc import *

# TODO split the concepts of "Job" and "JobInstance"
# this Job class is the combination of both
# a Job should have JobInstances and each JobInstance
# should have exactly one logfile, jid, etc
class Job:

	@staticmethod
	def fromDir(jobDir, interactive=False, quiet=False):
		if not os.path.isdir(jobDir):
			raise Exception('you must give a directory: ' + jobDir)
		home, name = os.path.split(jobDir)
		return Job(home, name, interactive, newjob=False, quiet=quiet)
	
	@staticmethod
	def jobsRunning():
		jobs = set()
		lines = subprocess.check_output('qstat').strip()
		if len(lines) == 0:
			return jobs
		ar = lines.split('\n')
		assert len(ar) >= 3	# first two lines are formatting
		for jstr in ar[2:]:
			jobs.add(int(jstr.split()[0]))
		return jobs

	@staticmethod
	def killEverything():
		for jid in Job.jobsRunning():
			os.system('qdel '+jid)

	def __init__(self, homeDir, name, f_hasFailed, interactive=False, newjob=True, quiet=False):
		"""
			homeDir is where experiments are stored
			name is the name of this experiment
				if there is another job with the same name, they are assumed
				to be the same. delete old jobs that you do not want to be confused with
			f_hasFailed is a function that takes a log file and returns a boolean for
				whether or not the job has failed
			if in interactive mode, will prompt user for input
				this should not be used for scripts, which would hang forever in these cases
		"""
		self.name = name
		self.home = os.path.join(homeDir, name)
		self.logDir = os.path.join(self.home, 'log')
		self.f_hasFailed = f_hasFailed

		if os.path.isfile(self.home):
			raise Exception('you the home directory you gave must not have any files or folders that match the name you gave')

		if newjob:
			print 'creating new job:', self.home
			if os.path.isdir(self.home):
				if interactive:
					print "the directory %s already exists" % (self.home)
					r = raw_input('would you like me to delete it for you? [y|n] ')
					if str2bool(r):
						os.system('rm -r ' + self.home)
						os.system('mkdir ' + self.home)
					else:
						raise Exception('cannot proceed')
				else:
					raise Exception('please give a unique name or delete old jobs: ' + self.home)
			elif 0 != os.system('mkdir -p ' + self.home):
				raise Exception('cannot make home directory!')

			if not os.path.isdir(self.logDir) and 0 != os.system('mkdir -p ' + self.logDir):
				raise Exception('cannot make log directory!')
		else:
			assert os.path.isdir(self.home)
			assert os.path.isdir(self.logDir)
			if not quiet: print 'loading existing job:', self.home

		self.javaOpt = FileDict(os.path.join(self.home, 'java.settings'), exists=not newjob)	# start with "-D"
		self.metaOpt = FileDict(os.path.join(self.home, 'meta.settings'), exists=not newjob)	# xmx, jar, profile, etc
		self.qsubOpt = FileDict(os.path.join(self.home, 'qsub.settings'), exists=not newjob)	# mem_free, h_rt

		self.prepared = False
	
	def getResourceDirectory(self, parentFolderName, childFolderName, overwrite=True):
		'''see getResourceFile for details'''
		folder = os.path.join(self.home, parentFolderName, childFolderName)
		if os.path.isdir(folder):
			if not overwrite: raise
		else:
			os.system('mkdir -p ' + folder)
		return folder

	def getResourceFile(self, folderName, fileName, overwrite=True):
		'''
			example usage: folderName='diagnostics', fileName='parameters.txt',
			this just returns a path to home/diagnostics/parameters.txt
			resources are usefule for job-specific output (avoid job output collision)
		'''
		folder = os.path.join(self.home, folderName)
		if not os.path.isdir(folder):
			os.system('mkdir ' + folder)
		f = os.path.join(folder, fileName)
		if not overwrite and os.path.isfile(f):
			raise Exception('this file already exists! ' + f)
		return f

	def addLib(self, dirOrFile):
		if os.path.isfile(dirOrFile):
			cp = selt.class_path() + ':' + dirOrFile
			self.metaOpt.setValue('class_path', cp)
		else:
			l = all_jars_in(dirOrFile)
			l.append(self.class_path())
			self.metaOpt.setValue('class_path', ':'.join(l))

	def jar(self):
		return self.metaOpt.getValue('jar')
	
	def main_class(self):
		return self.metaOpt.getValue('main_class')
	
	def class_path(self):
		return self.metaOpt.getValue('class_path')
	
	def xmx(self):
		return self.metaOpt.getValue('xmx')
	
	def profile(self):
		return self.metaOpt.getValue('profile', 'n') == 'y'
	
	def mem_free(self):
		return self.qsubOpt.getValue('mem_free')

	def use_asserts(self):
		return str2bool(self.metaOpt.getValue('asserts'))

	def command_line_args(self):
		f = codecs.open(os.path.join(self.home, 'command_line_args.txt'), 'r', 'utf-8')
		args = [x.strip() for x in f.readlines()]
		f.close()
		return args

	def setJavaOption(self, key, value):
		self.javaOpt.setValue(key, value)

	def qsubScript(self):
		return os.path.join(self.home, 'job.sh')
	
	def writeQsubScript(self, cmd):
		f = self.qsubScript()
		ff = codecs.open(f, 'w', 'utf-8')
		ff.write("#$ -cwd\n")	# run from current directory
		ff.write("#$ -j y\n")	# join stderr to stdout
		ff.write("#$ -V\n")
		ff.write("#$ -l h_rt=72:00:00\n")	# timeout
		ff.write("#$ -l mem_free=%s\n" % (self.mem_free()))
		ff.write("#$ -M twolfe18@gmail.com\n")
		ff.write("#$ -m as\n") # a=aborted b=begining e=end s=suspended
		ff.write("#$ -o %s\n" % self.logDir)
		ff.write(cmd + " && echo -e \"finished\\t`date +\"%%Y-%%m-%%d %%H:%%M:%%S\"`\" >> %s\n" % (self.metaOpt.filename))
		ff.write('\n')
		ff.close()
		return f
	
	def prepare(self):

		# java and jar
		cmd = 'java'
		if self.jar() != 'None':
			if not os.path.isfile(self.jar()):
				raise Exception('JAR file provide is not a file! ' + self.jar())
			cmd += " -jar %s \\\n\t" % (self.jar())
			jarMD5 = subprocess.check_output("sha1sum %s" % (self.jar()), shell=True).strip()
			self.metaOpt.setValue('jar-sha1', jarMD5)

		# class path, assert
		cmd += ' -cp ' + self.class_path() + ' \\\n\t'
		#cmd += ' -cp ' + self.class_path() + ':' + self.home + ' \\\n\t'
		if self.use_asserts():
			cmd += ' -ea \\\n\t'

		# profiling
		if self.profile():
			cmd += ' -agentlib:hprof=cpu=samples,depth=20,heap=sites \\\n\t'

		# java options
		for k,v in self.javaOpt.iteritems():
			if not k.startswith('-D'):
				k = '-D'+k
			cmd += " %s=\"%s\" \\\n\t" % (k,v)

		# main class
		cmd += ' '+self.main_class() + ' \\\n\t'

		# command line arguments
		args = self.command_line_args()
		for a in args:
			cmd += " %s \\\n\t\t" % (a)

		# generate a shell script for SGE
		self.writeQsubScript(cmd)
		self.prepared = True
	
	def setSubmission(self, class_path, main_class, args, jar=None, xmx='2G', mem_free='3G', \
			profile=False, asserts=True, actuallySubmit=False):

		f = codecs.open(os.path.join(self.home, 'command_line_args.txt'), 'w', 'utf-8')
		for a in args:
			f.write(a + '\n')
		f.close()

		xmx = canonicalMemoryDescription(xmx)
		mem_free = canonicalMemoryDescription(mem_free)

		if jar is None: jar = 'None'
		self.metaOpt.setValue('jar', jar)
		self.metaOpt.setValue('main_class', main_class)
		self.metaOpt.setValue('class_path', class_path)
		self.metaOpt.setValue('xmx', xmx)
		self.qsubOpt.setValue('mem_free', mem_free)
		self.metaOpt.setValue('profile', bool2str(profile))
		self.metaOpt.setValue('asserts', bool2str(asserts))

		self.prepare()
		if actuallySubmit:
			self.submit()
	
	def submit(self):
		# don't submit the same job twice
		assert self.submittedAt() is None
		assert self.jid() is None
		assert self.prepared
		qsubScript = self.qsubScript()
		self.metaOpt.setValue('submitted', timestamp())
		self.metaOpt.flush()
		self.javaOpt.flush()
		self.qsubOpt.flush()
		r = subprocess.check_output("qsub -N %s %s" % (self.name, qsubScript), shell=True)
		jid = int(r.split()[2])
		self.metaOpt.setValue('jid', jid, flush=True)
		print "submitted job \"%s\" (%d)" % (self.name, jid)

	def submittedAt(self):
		s = self.metaOpt.getValue('submitted')
		if s: return parsetime(s)
		else: return None

	def jid(self):
		s = self.metaOpt.getValue('jid')
		if s: return int(s)
		else: return None
	
	def failed(self):
		if self.recentlySubmitted():
			return False
		f = self.newestLog()
		if not f:
			print 'there is no log file, assuming it failed!'
		else:
			return self.f_hasFailed(f)

	def recentlySubmitted(self):
		'''
		give qsub some time to get its stuff together...
		for the first 5 seconds after submitting, just assume the
		job is running normally
		'''
		assert self.submittedAt() is not None
		return time.time() - self.submittedAt() < 10.0

	def isFinished(self):
		if self.recentlySubmitted() or self.isRunning():
			return False
		self.metaOpt.load()
		return self.metaOpt.hasKey('finished')

	def isRunning(self):
		if self.recentlySubmitted():
			return True
		assert self.jid() is not None
		assert type(self.jid()) is int
		running = Job.jobsRunning()
		return self.jid() in running

	def wait(self, timeout=48*60*60, secsBetweenPolls=30, exceptionOnTimeout=False):
		assert self.jid() is not None
		assert type(self.jid()) is int
		total = 0
		while total < timeout and self.isRunning():
			total += secsBetweenPolls
			time.sleep(secsBetweenPolls)
		if exceptionOnTimeout and total >= timeout:
			raise Exception("waited for %d seconds and job is not done!" % (total))
		else:
			print "waited %d seconds, but %d is done!" % (total, self.jid())

	def kill(self):
		if not self.isRunning():
			raise Exception('jid is none, job is not live')
		assert self.jid() is not None
		assert type(self.jid()) is int
		os.system("qdel %d" % (self.jid()))

	def pause(self):
		if not self.isRunning():
			raise Exception('jid is none, job is not live')
		assert self.jid() is not None
		assert type(self.jid()) is int
		os.system("qalter -u %d" % (self.jid()))

	def unpause(self):
		if not self.isRunning():
			raise Exception('jid is none, job is not live')
		assert self.jid() is not None
		assert type(self.jid()) is int
		os.system("qalter -U %d" % (self.jid()))

	def logs(self):
		assert self.logDir is not None and os.path.isdir(self.logDir)
		return [os.path.join(self.logDir, x) for x in os.listdir(self.logDir)]
	
	def newestLog(self):
		l = self.logs()
		if len(l) == 0:
			raise Exception('there are no logs!')
		return l[-1]


# TODO
# make a meta directory next to job directories which
	# 1. contains a jar file
		# to support this, have method like addSourceJar(jarfile)
		# which copies that jarfile into the metadirectory (if sha1 doesn't match a previous one)
	# 2. should be able to specify where this goes
		# maybe want to have a global cache where you can share JAR file versions






