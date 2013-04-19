
import re

class FeatureSelection:
	'''
	abstract implementation of forward selection
	you provide an evaluation function and it does the algorithm
	'''

	def __init__(self, evaluateFeatures):
		'''evaluateFeatures: List[String] => Double'''
		self.evalFeatures = evaluateFeatures

	def forwardSelection(self, allFeatures, initFeatures=[], minImprovement=0.03, maxIter=None):
		if maxIter is None: maxIter = len(allFeatures)
		cur = initFeatures
		improvements = {}
		remaining = allFeatures
		old_performance = 0.0
		for i in range(min(maxIter, len(allFeatures))):
			newFeat, performance = self.oneStep(cur, remaining)
			improvement, old_performance = performance - old_performance, performance
			print "iter=%d feature=%s improvement=%.3f" % (i, newFeat, improvement)
			if improvement > minImprovement:
				print "adding feature %s" % (newFeat)
				cur.append(newFeat)
				remaining.remove(newFeat)
				improvements[newFeat] = improvement
			else:
				print "gain insignificant (%.3f < %.3f), done" % (improvement, minImprovement)
				break
		return (cur, improvements)

	def oneStep(self, curFeatures, nextFeatures):
		'''adds the best feature from nextFeatures to curFeatures'''
		baseline = self.evalFeatures(curFeatures)
		best = (None, baseline)
		for newFeat in nextFeatures:
			workingWith = curFeatures + [newFeat]
			p = self.evalFeatures(workingWith)
			if p > best[1]:
				best = (newFeat, p)
		return best

class FeatureSelectionWithJobManagement(FeatureSelection):
	'''
	forward feature selction with job control
	you provide a function that generates a job given features,
	and this implements the waiting logic
	'''

	def __init__(self, f_evaluate, f_jobWithFeatures):
		'''
		f_evaluate: Job => Double	// higher is better
		f_jobWithFeatures: List[String] => Job	// unsubmitted job
		'''
		self.f_evaluate = f_evaluate
		self.f_jobWithFeatures = f_jobWithFeatures
	
	def oneStep(self, curFeatures, nextFeatures):
		'''adds the best feature from nextFeatures to curFeatures'''

		# submit jobs
		jobs = []
		for newFeat in nextFeatures:
			workingWith = curFeatures + [newFeat]
			j = self.f_jobWithFeatures(workingWith)
			jobs.append(j)
			j.submit()

		# wait for jobs
		mins = 10
		print "waiting up to %d minutes for %d jobs too complete..." % (mins, len(jobs))
		for j in jobs:
			j.wait(secsBetweenPolls=10, timeout=mins*60, exceptionOnTimeout=True)
			assert j.isFinished()
			if j.failed():
				raise Exception(j + ' failed!')
		print 'all jobs finished!'
		
		# evaluate jobs
		best = (None, 0.0)
		for i, newFeat in enumerate(nextFeatures):
			p = self.f_evaluate(jobs[i])
			print "[FeatureSelectionWithJobManagement] feature=%s perf=%f" % (newFeat, p)
			if p > best[1] or best[0] is None:
				print 'new best feature:', newFeat, p
				best = (newFeat, p)

		return best
		


