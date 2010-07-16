#!/usr/bin/python

import os, sys, time, re
import apsw
from proc import *

connection=apsw.Connection(":memory:")
cursor=connection.cursor()

connection.createmodule("process_fd_source", ProcessFdSource())
connection.createmodule("process_stat_source", ProcessStatSource())
connection.createmodule("process_statm_source", ProcessStatmSource())
connection.createmodule("process_cmdline_source", ProcessCmdLineSource())
connection.createmodule("process_oom_score_source", ProcessOomScoreSource())
connection.createmodule("process_cwd_source", ProcessCwdSource())
connection.createmodule("process_root_source", ProcessRootSource())
connection.createmodule("process_exe_source", ProcessExeSource())
connection.createmodule("process_schedstat_source", ProcessSchedStatSource())

cursor.execute("create virtual table process_fd using process_fd_source()")
cursor.execute("create virtual table process_stat using process_stat_source()")
cursor.execute("create virtual table process_statm using process_statm_source()")
cursor.execute("create virtual table process_cmdline using process_cmdline_source()")
cursor.execute("create virtual table process_oom_score using process_oom_score_source()")
cursor.execute("create virtual table process_cwd using process_cwd_source()")
cursor.execute("create virtual table process_root using process_root_source()")
cursor.execute("create virtual table process_exe using process_exe_source()")
cursor.execute("create virtual table process_schedstat using process_schedstat_source()")

for pid, comm, cmdline in cursor.execute("select ps1.pid, ps1.comm, ps2.cmdline from process_stat as ps1 inner join process_cmdline as ps2 on ps1.pid = ps2.pid order by ps1.comm"):
	print pid, comm

for pid, fd, path in cursor.execute("select ps.pid, ps.comm, fd.path from process_fd as fd inner join process_stat as ps on fd.pid = ps.pid where ps.comm like '%a%' order by fd.path"):
	print pid, fd, path

for pid, cwd, root, exe in cursor.execute("select cwd.pid, cwd.cwd, root.root, exe.exe from process_cwd as cwd inner join process_root as root on cwd.pid = root.pid inner join process_exe as exe on cwd.pid = exe.pid"):
	print pid, cwd, root, exe

for pid, a, b, c in cursor.execute("select * from process_schedstat"):
	print pid, a, b, c


#for a, b, c, d, e, f, g, h in cursor.execute("select * from process_statm"):
#	print a, b, c, d, e, f, g, h

#for pid, oom_score in cursor.execute("select pid, oom_score from process_oom_score"):
#	print pid, oom_score

#for pid, cmdline in cursor.execute("select pid, cmdline from process_cmdline"):
#	print pid, cmdline

connection.close(True)  # force it since we want to exit


