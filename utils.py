import datetime
import time

def create_a_timestamp():
	time.sleep(1) # This is to ensure that no two timestamps are the same.
	return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def interlace(lst):
	# https://en.wikipedia.org/wiki/Interlacing_(bitmaps)
	lst = sorted(lst)[::-1]
	result = [lst[0], lst[-1]]
	ranges = [(1, len(lst) - 1)]
	for start, stop in ranges:
		if start < stop:
			middle = (start + stop) // 2
			result.append(lst[middle])
			ranges += (start, middle), (middle + 1, stop)
	return result
