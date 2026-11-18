import os, sys, pdb, shutil, logging, tempfile, re, time
from pathlib import Path

import unittest as test

from nistoar.pdr.publish.service import monitor, status

tmpdir = tempfile.TemporaryDirectory(prefix="_test_monitor.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_nsd.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

class TestPublishingMonitorQueue(test.TestCase):

    def setUp(self):
        self.qfile = os.path.join(tmpdir.name, "monitorq.tsv")

    def tearDown(self):
        if os.path.isfile(self.qfile):
            os.remove(self.qfile)

    def test_ctor(self):
        wrap = monitor.PublishingMonitorQueue(Path(self.qfile))
        self.assertTrue(wrap._file)
        self.assertTrue(not wrap._file.exists())

    def test_add_SIP(self):
        q = monitor.PublishingMonitorQueue(self.qfile)
        q.add_SIP("pdr0:2000")
        self.assertTrue(q._file.is_file())
        sips = q.read_data()
        self.assertEqual(len(sips), 1)
        self.assertIn("pdr0:2000", sips)
        self.assertEqual(sips["pdr0:2000"], ('', ''))

        q.add_SIP("pdr0:2000", "wondering")
        sips = q.read_data()
        self.assertEqual(len(sips), 1)
        self.assertIn("pdr0:2000", sips)
        self.assertEqual(sips["pdr0:2000"], ('wondering', ''))

        q.add_SIP("pdr0:2001", "wondering", "")
        q.add_SIP("pdr0:2002", "waiting", "how much longer?!")
        sips = q.read_data()
        self.assertEqual(len(sips), 3)
        self.assertEqual(sips["pdr0:2000"], ('wondering', ''))
        self.assertEqual(sips["pdr0:2001"], ('wondering', ''))
        self.assertEqual(sips["pdr0:2002"], ('waiting', 'how much longer?!'))
        
class TestLocalPublishingMonitor(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_work.")
        self.statusdir = os.path.join(self.tmpdir.name, "status")
        os.mkdir(self.statusdir)
        self.qfile = os.path.join(self.tmpdir.name, "monitorq.tsv")
        self.changefile = os.path.join(self.tmpdir.name, "change.tsv")
        self.monitor = monitor.LocalPublishingMonitor(self.statusdir, self.qfile, self.onchange, 0.2)
        self.sips = ["mds2:500"+str(i) for i in range(3)]
        self.client = monitor.FileBasedPublishingMonitorClient(self.qfile)

    def onchange(self, sipid, state, msg):
        monitor.PublishingMonitorQueue(self.changefile).add_SIP(sipid, state, msg)        

    def tearDown(self):
        if self.monitor.is_running():
            self.monitor._monthread.join(6)
        self.tmpdir.cleanup()

    def make_sips(self):
        s = status.SIPStatus(self.sips[0], self.statusdir)
        s.start("pdr0", message="hello")
        s = status.SIPStatus(self.sips[1], self.statusdir)
        s.start("pdr0", message="bon jour")
        s.update(status.SUBMITTED, "waiting")
        s = status.SIPStatus(self.sips[2], self.statusdir)
        s.start("pdr0")
        s.update(status.PROCESSING, "working")

    def test_ctor(self):
        self.assertEqual(str(self.monitor._qfile), self.qfile)
        self.assertTrue(os.path.exists(self.qfile))
        self.assertTrue(self.monitor._onchange)
        self.assertEqual(self.monitor._in_queue, [])

        self.assertEqual(str(self.client.qfile), self.qfile)

        self.make_sips()
        for sf in ["mds2:500"+str(i) for i in range(3)]:
            self.assertTrue(os.path.isfile(os.path.join(self.statusdir, sf+".json")))

    def test_client(self):
        self.client.watch(self.sips[0], status.SUBMITTED, "ready")
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 1)
        self.assertIn(self.sips[0], q)
        self.assertEqual(q[self.sips[0]], (status.SUBMITTED, 'ready'))

        self.client.watch(self.sips[1])
        self.client.watch(self.sips[2])
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 3)
        self.assertIn(self.sips[0], q)
        self.assertEqual(q[self.sips[0]], (status.SUBMITTED, 'ready'))
        self.assertEqual(q[self.sips[1]], ('', ''))
        self.assertEqual(q[self.sips[2]], ('', ''))

    def test_update_statuses_in_queue(self):
        self.assertTrue(not os.path.exists(self.changefile))
        self.assertEqual(monitor.PublishingMonitorQueue.read(self.qfile), {})
        self.monitor.update_statuses_in_queue()
        self.assertEqual(monitor.PublishingMonitorQueue.read(self.qfile), {})

        # monitor non-existent SIP
        self.client.watch(self.sips[1])
        self.assertEqual(monitor.PublishingMonitorQueue.read(self.qfile)[self.sips[1]], ('', ''))
        self.monitor.update_statuses_in_queue()
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 0)   # removed because state is NOT_FOUND
        
        self.assertTrue(os.path.isfile(self.changefile))
        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 1)
        self.assertEqual(ch[self.sips[1]][0], status.NOT_FOUND)

        # watch submitted one
        self.make_sips()
        self.client.watch(self.sips[1])
        self.assertEqual(monitor.PublishingMonitorQueue.read(self.qfile)[self.sips[1]], ('', ''))
        self.monitor.update_statuses_in_queue()
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 1)
        self.assertEqual(q[self.sips[1]][0], status.SUBMITTED)

        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 1)
        self.assertEqual(ch[self.sips[1]][0], status.SUBMITTED)

        self.client.watch(self.sips[0], status.PROCESSING, 'work work work')
        self.assertEqual(len(monitor.PublishingMonitorQueue.read(self.qfile)), 2)
        self.monitor.update_statuses_in_queue()
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 2)
        self.assertEqual(q[self.sips[1]][0], status.SUBMITTED)
        self.assertEqual(q[self.sips[0]][0], status.PROCESSING)
        self.assertEqual(q[self.sips[0]][1], "work work work")

        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 1)
        self.assertEqual(ch[self.sips[1]][0], status.SUBMITTED)

        self.client.watch(self.sips[2])
        self.assertEqual(len(monitor.PublishingMonitorQueue.read(self.qfile)), 3)
        self.monitor.update_statuses_in_queue()
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 3)
        self.assertEqual(q[self.sips[1]][0], status.SUBMITTED)
        self.assertEqual(q[self.sips[0]][0], status.PROCESSING)
        self.assertEqual(q[self.sips[0]][1], "work work work")
        self.assertEqual(q[self.sips[2]][0], status.PROCESSING)

        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 2)
        self.assertEqual(ch[self.sips[1]][0], status.SUBMITTED)
        self.assertEqual(ch[self.sips[2]][0], status.PROCESSING)

        # update the status
        s = status.SIPStatus(self.sips[0], self.statusdir).update(status.ONHOLD, "whoa")
        self.monitor.update_statuses_in_queue()
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 3)
        self.assertEqual(q[self.sips[0]][0], status.ONHOLD)    # stays in queue
        self.assertEqual(q[self.sips[0]][1], "whoa")
        self.assertEqual(q[self.sips[1]][0], status.SUBMITTED)
        self.assertEqual(q[self.sips[2]][0], status.PROCESSING)

        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 3)
        self.assertEqual(ch[self.sips[0]][0], status.ONHOLD)
        self.assertEqual(ch[self.sips[1]][0], status.SUBMITTED)
        self.assertEqual(ch[self.sips[2]][0], status.PROCESSING)

        s = status.SIPStatus(self.sips[1], self.statusdir).update(status.FAILED, "darn")
        self.monitor.update_statuses_in_queue()
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 2)
        self.assertEqual(q[self.sips[0]][0], status.ONHOLD)
        self.assertEqual(q[self.sips[2]][0], status.PROCESSING)

        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 3)
        self.assertEqual(ch[self.sips[0]][0], status.ONHOLD)
        self.assertEqual(ch[self.sips[1]][0], status.FAILED)
        self.assertEqual(ch[self.sips[1]][1], "darn")
        self.assertEqual(ch[self.sips[2]][0], status.PROCESSING)

        s = status.SIPStatus(self.sips[2], self.statusdir).update(status.PUBLISHED, "Yay!")
        self.monitor.update_statuses_in_queue()
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 1)
        self.assertEqual(q[self.sips[0]][0], status.ONHOLD)

        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 3)
        self.assertEqual(ch[self.sips[0]][0], status.ONHOLD)
        self.assertEqual(ch[self.sips[1]][0], status.FAILED)
        self.assertEqual(ch[self.sips[1]][1], "darn")
        self.assertEqual(ch[self.sips[2]][0], status.PUBLISHED)
        self.assertEqual(ch[self.sips[2]][1], "Yay!")

    def test_monitor_emptystop(self):
        self.make_sips()
        self.assertTrue(not os.path.exists(self.changefile))

        self.client.watch(self.sips[0])

        # launch monitor
        self.assertTrue(not self.monitor.is_running())
        launchtime = time.time()
        self.monitor.launch_monitoring(True, timeout=5)
        self.assertGreater(launchtime + 5, time.time())
        self.assertTrue(self.monitor.is_running())
        time.sleep(0.25)

        # should process sips[0] on first cycle
        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 1)
        self.assertEqual(ch[self.sips[0]][0], status.PROCESSING)

        # add second sip
        self.client.watch(self.sips[1], status.SUBMITTED)
        time.sleep(0.25)
        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 1)
        self.assertEqual(ch[self.sips[0]][0], status.PROCESSING)
        
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 2)
        self.assertEqual(q[self.sips[0]][0], status.PROCESSING)
        self.assertEqual(q[self.sips[1]][0], status.SUBMITTED)
        self.assertTrue(self.monitor.is_running())

        # update status on first sip
        status.SIPStatus(self.sips[0], self.statusdir).update(status.PUBLISHED, "whoa")
        time.sleep(0.25)
        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 1)
        self.assertEqual(ch[self.sips[0]][0], status.PUBLISHED)
        
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 1)
        self.assertEqual(q[self.sips[1]][0], status.SUBMITTED)
        self.assertTrue(self.monitor.is_running())

        # update status on second sip
        status.SIPStatus(self.sips[1], self.statusdir).update(status.FAILED, "oops!")
        time.sleep(0.25)
        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 2)
        self.assertEqual(ch[self.sips[0]][0], status.PUBLISHED)
        self.assertEqual(ch[self.sips[1]][0], status.FAILED)
        
        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 0)
        self.assertTrue(not self.monitor.is_running())

    def test_monitor_once(self):
        self.make_sips()
        self.assertTrue(not os.path.exists(self.changefile))

        self.client.watch(self.sips[0])

        # launch monitor
        self.assertTrue(not self.monitor.is_running())
        launchtime = time.time()
        self.monitor.launch_monitoring(1, timeout=5)
        self.assertGreater(launchtime + 5, time.time())
        time.sleep(0.25)
        self.assertTrue(not self.monitor.is_running())

        # should process sips[0] on first cycle
        ch = monitor.PublishingMonitorQueue.read(self.changefile)
        self.assertEqual(len(ch), 1)
        self.assertEqual(ch[self.sips[0]][0], status.PROCESSING)

        q = monitor.PublishingMonitorQueue.read(self.qfile)
        self.assertEqual(len(q), 1)
        self.assertEqual(q[self.sips[0]][0], status.PROCESSING)
        

if __name__ == '__main__':
    test.main()


        
        

