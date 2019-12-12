import time


class Timer(object):
    def __init__(self):
        self.texec = 0
        self.tpersist = 0
        self.tload = 0
        self.tindex = 0
        self.tsearch = 0
        self.ttotal = 0
        self.tload_raw = 0

    def update_texec(self, ttmp):
        self.texec = self.texec + (time.clock() - ttmp)

    def update_tpersist(self, ttmp):
        self.tpersist = self.tpersist + (time.clock() - ttmp)

    def update_tload(self, ttmp):
        self.tload = self.tload+ (time.clock() - ttmp)

    def update_tindex(self, ttmp):
        self.tindex = self.tindex + (time.clock() - ttmp)

    def update_tsearch(self, ttmp):
        self.tsearch = self.tsearch + (time.clock() - ttmp)

    def update_ttotal(self, ttmp):
        self.ttotal = self.ttotal +(time.clock() - ttmp)

    def update_tload_raw(self, ttmp):
        self.tload_raw = self.tload_raw +(time.clock() - ttmp)


def write_timer_irods(timer, timer_path, irods_sess):

    try:
        if not irods_sess.data_objects.exists(timer_path):
            irods_sess.data_objects.create(timer_path)
        obj = irods_sess.data_objects.get(timer_path)
        with obj.open('a') as index_file:
            index_file.seek(0, 2)
            index_file.write(str([timer.ttotal, timer.texec, timer.tpersist, timer.tindex, timer.tload, timer.tload_raw, timer.tsearch])[1:-1] +"\n")

    except EnvironmentError, e:  # parent of IOError, OSError *and* WindowsError where available
        print e
