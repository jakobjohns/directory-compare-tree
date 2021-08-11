''' The syntax for this script is simply "dir_cmp.py <dir1> <dir2>'''

import os, sys
import hashlib
import stat
import multiprocessing
from multiprocessing import Process, JoinableQueue

num_procs = multiprocessing.cpu_count()
num_procs *= 4

def get_hash(fh, sz):
        h = hashlib.sha256()
        while True:
                buf = fh.read(65536)
                if len(buf) == 0: break
                h.update(buf)
        return h.digest()

def chk(path, lf, dir):
        path1 = os.path.join(dir, path)
        st = None
        st1 = None
        try:
                st = os.lstat(path)
                st1 = os.lstat(path1)
        except:
                lf.write("Missing: " + path1 + "\n")
                lf.flush()
                return

        if not stat.S_ISREG(st.st_mode):
                return
        if st.st_size != st1.st_size:
                lf.write("Size differ: " + path1 + "\n")
                lf.flush()
                return
        if st.st_size == 0: return

        hv = None
        hv1 = None
        try:
                fh = open(path, "r")
                hv = get_hash(fh, st.st_size)
                fh.close()
                fh = open(path1, "r")
                hv1 = get_hash(fh, st.st_size)
                fh.close()
        except:
                lf.write("Open error: " + path1 + "\n")
                lf.flush()
                return

        if hv != hv1:
                lf.write("Digests differ: " + path1 + "\n")
                lf.flush()

def proc_chk(q, lf, dir):
        while True:
                path1 = q.get()
                if path1 == "done":
                        break
                chk(path1, lf, dir)
                q.task_done()
        q.task_done()

q = JoinableQueue()
lf = open("/var/tmp/differ_files", "w+")
dir1 = os.path.realpath(sys.argv[1])
dir2 = os.path.realpath(sys.argv[2])

o_cwd = os.getcwd()
os.chdir(dir1)
cwd = os.getcwd()

for i in range(0, num_procs):
        p = Process(target=proc_chk, args=(q, lf, dir2))
        p.start()

for dirpath, dirnames, filenames in os.walk(".", followlinks=False):
        for f in filenames:
                q.put(os.path.join(dirpath, f))

for i in range(0, num_procs):
        q.put("done")#q.join()
lf.close()
os.chdir(o_cwd)