
import os, codecs

class FileDict:

	def __init__(self, filename, exists=False):
		self.filename = filename
		self.dirty = False	# have we written mem->disk?
		if exists:
			self.load(assertFindOnDisk=True)
		else:
			assert not os.path.exists(filename)
			self.dictionary = {}
			os.system('touch ' + filename)
	
	def existsOnDisk(self):
		return os.path.isfile(self.filename)

	def setValue(self, key, value, allowOverwrite=True, flush=False):
		if '\t' in key or '\t' in str(value):
			raise Exception('you cannot have tabs in your keys or values')
		if not self.dictionary:
			self.load()
		if not allowOverwrite and key in self.dictionary:
			raise Exception("you already have %s = %s, cannot set to %s" % (key, self.dictionary[key], value))
		self.dictionary[key] = value
		if flush:
			self.flush()
		else:
			self.dirty = True
	
	def hasKey(self, key):
		return key in self.dictionary

	def getValue(self, key, default=None):
		if not self.dictionary:
			self.load()
		if key in self.dictionary:
			return self.dictionary[key]
		else:
			return default
	
	def iteritems(self):
		if not self.dictionary:
			self.load()
		return self.dictionary.iteritems()
	
	def load(self, assertFindOnDisk=False):
		assert not self.dirty
		assert not assertFindOnDisk or self.existsOnDisk()
		f = codecs.open(self.filename, 'r', 'utf-8')
		self.dictionary = {}
		for line in f:
			ar = line.strip().split('\t')
			assert len(ar) == 2
			self.dictionary[ar[0]] = ar[1]
		f.close()
	
	def flush(self):
		assert self.dictionary
		f = codecs.open(self.filename, 'w', 'utf-8')
		for k,v in self.dictionary.iteritems():
			f.write("%s\t%s\n" % (k,v))
		f.close()
		self.dirty = False

