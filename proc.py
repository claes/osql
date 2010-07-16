import os, sys, time, re

import apsw

schedstat_columns = ['pid', 
	'avgrun',
	'avgwait',
	'nooftasks']

statm_columns = ['pid', 
	'size',
	'resident',
	'share',
	'text',
	'lib',
	'data',
	'dt']

stats_columns = ['pid',
	'comm',
	'state',
	'ppid',
	'pgrp',
	'session',
	'tty_nr',
	'tpgid',
	'flags',
	'minflt',
	'cminflt',
	'majflt',
	'cmajflt',
	'utime',
	'stime',
	'cutime',
	'cstime',
	'priority',
	'nice',
	'num_threads',
	'iterealvalue',
	'starttime',
	'vsize',
	'rss',
	'rsslim',
	'startcode',
	'endcode',
	'startstack',
	'kstkeip',
	'signal',
	'blocked',
	'sigignore',
	'sigcatch',
	'wchan',
	'nswap',
	'cnswap',
	'exit_signal',
	'processor',
	'rt_priority',
	'policy',
	'delayacct_blkio_ticks',
	'guest_time',
	'cguest_time']

class ProcDirIter: 
	def __init__(self):
		self.procList = []
		for f in os.listdir('/proc'):
			processdir = os.path.join('/proc', f)
			if os.path.isdir(processdir) and re.match('/proc/[0-9]+', processdir):
				self.procList.append([int(f), processdir])

	def __iter__(self):
		return self
	
	def next(self):
		if (len(self.procList) > 0):
			return self.procList.pop()
		else: 
			raise StopIteration()

class FdDirIter: 
	def __init__(self, dir):
		self.fdList = []
		for f in os.listdir(dir):
			path = os.path.join(dir, f)
			if os.path.islink(path) and re.match('[0-9]+', f):
				self.fdList.append([int(f), os.readlink(path)])

	def __iter__(self):
		return self
	
	def next(self):
		if (len(self.fdList) > 0):
			return self.fdList.pop()
		else: 
			raise StopIteration()

def getProcessFileDescriptors():
	data = []
	for pid, processdir in ProcDirIter():
		for fd, path in FdDirIter(os.path.join(processdir, 'fd')):
			row = []
			row.append(pid)
			row.append(fd)
			row.append(path)
			data.append(row)
	return ['pid', 'fd', 'path'], data


def getProcessCmdLine():
	data = []
	for pid, processdir in ProcDirIter():
		cmdfile = open(os.path.join(processdir, 'cmdline'), 'r')
		cmdline = cmdfile.readline()
		cmdfile.close()
		row = []
		row.append(pid)
		row.append(cmdline)
		data.append(row)
	return ['pid', 'cmdline'], data

def getProcessCwd():
	data = []
	for pid, processdir in ProcDirIter():
		cwd = os.readlink(os.path.join(processdir, 'cwd'))
		row = []
		row.append(pid)
		row.append(cwd)
		data.append(row)
	return ['pid', 'cwd'], data

def getProcessExe():
	data = []
	for pid, processdir in ProcDirIter():
		try:
			exe = os.readlink(os.path.join(processdir, 'exe'))
		except: 
			exe = None
		row = []
		row.append(pid)
		row.append(exe)
		data.append(row)
	return ['pid', 'exe'], data


def getProcessRoot():
	data = []
	for pid, processdir in ProcDirIter():
		root = os.readlink(os.path.join(processdir, 'root'))
		row = []
		row.append(pid)
		row.append(root)
		data.append(row)
	return ['pid', 'root'], data

def getProcessOomScore():
	data = []
	for pid, processdir in ProcDirIter():
		oom_score_file = open(os.path.join(processdir, 'oom_score'), 'r')
		oom_score = int(oom_score_file.readline().rstrip())
		oom_score_file.close()
		row = []
		row.append(pid)
		row.append(oom_score)
		data.append(row)
	return ['pid', 'oom_score'], data

def getProcessSchedStat():
	data = []
	for pid, processdir in ProcDirIter():
		schedstatfile = open(os.path.join(processdir, 'schedstat'), 'r')
		schedstatline = schedstatfile.readline()
		schedstatfile.close()
		schedstat = schedstatline.rstrip().split(' ')
		i = 0
		row = []
		for column in schedstat_columns:
			if i == 0:
				row.append(pid)
			else: 
				row.append(schedstat[i-1])
			i = i+1
		data.append(row)
	return schedstat_columns, data

def getProcessStat():
	data = []
	for pid, processdir in ProcDirIter():
		statfile = open(os.path.join(processdir, 'stat'), 'r')
		statline = statfile.readline()
		statfile.close()
		stats = statline.split(' ')
		i = 0
		row = []
		for column in stats_columns:
			row.append(stats[i])
			i = i+1
		data.append(row)
	return stats_columns, data

def getProcessStatm():
	data = []
	for pid, processdir in ProcDirIter():
		statfile = open(os.path.join(processdir, 'statm'), 'r')
		statline = statfile.readline()
		statfile.close()
		stats = statline.rstrip().split(' ')
		i = 0
		row = []
		for column in statm_columns:
			if i == 0:
				row.append(pid)
			else: 
				row.append(stats[i-1])
			i = i+1
		data.append(row)
	return statm_columns, data

class ProcessStatSource:
	def Create(self, db, modulename, dbname, tablename, *args):
		columns,data=getProcessStat()
		schema="create table process_stats("+','.join(["'%s'" % (x,) for x in stats_columns[0:]])+")"
		return schema,Table(columns,data)
	Connect=Create

class ProcessStatmSource:
	def Create(self, db, modulename, dbname, tablename, *args):
		columns,data=getProcessStatm()
		schema="create table process_statm("+','.join(["'%s'" % (x,) for x in statm_columns[0:]])+")"
		return schema,Table(columns,data)
	Connect=Create

class ProcessSchedStatSource:
	def Create(self, db, modulename, dbname, tablename, *args):
		columns,data=getProcessSchedStat()
		schema="create table process_schedstat("+','.join(["'%s'" % (x,) for x in schedstat_columns[0:]])+")"
		return schema,Table(columns,data)
	Connect=Create

class ProcessCmdLineSource:
	def Create(self, db, modulename, dbname, tablename, *args):
		columns,data=getProcessCmdLine()
		schema="create table process_cmdline('pid' integer, 'cmdline')"
		return schema,Table(columns,data)
	Connect=Create

class ProcessCwdSource:
	def Create(self, db, modulename, dbname, tablename, *args):
		columns,data=getProcessCwd()
		schema="create table process_cwd('pid' integer, 'cwd')"
		return schema,Table(columns,data)
	Connect=Create

class ProcessExeSource:
	def Create(self, db, modulename, dbname, tablename, *args):
		columns,data=getProcessExe()
		schema="create table process_exe('pid' integer, 'exe')"
		return schema,Table(columns,data)
	Connect=Create

class ProcessRootSource:
	def Create(self, db, modulename, dbname, tablename, *args):
		columns,data=getProcessRoot()
		schema="create table process_root('pid' integer, 'root')"
		return schema,Table(columns,data)
	Connect=Create

class ProcessOomScoreSource:
	def Create(self, db, modulename, dbname, tablename, *args):
		columns,data=getProcessOomScore()
		schema="create table process_oomscore('pid' integer, 'oom_score' integer)"
		return schema,Table(columns,data)
	Connect=Create

class ProcessFdSource:
	def Create(self, db, modulename, dbname, tablename, *args):
		columns,data=getProcessFileDescriptors()
		schema="create table process_filedescriptors('pid', 'fd', 'path')"
		return schema,Table(columns,data)
	Connect=Create

class Table:
	def __init__(self, columns, data):
	    self.columns=columns
	    self.data=data

	def BestIndex(self, *args):
	    return None

	def Open(self):
	    return Cursor(self)

	def Disconnect(self):
	    pass

	Destroy=Disconnect

class Cursor:
	def __init__(self, table):
	    self.table=table

	def Filter(self, *args):
	    self.pos=0

	def Eof(self):
	    return self.pos>=len(self.table.data)

	def Rowid(self):
	    return self.table.data[self.pos][0]

	def Column(self, col):
	    return self.table.data[self.pos][col]

	def Next(self):
	    self.pos+=1

	def Close(self):
	    pass


