import time
import struct

class DummyFileHandle:
  def seek(self, offset):
    print("seek %d" % offset)

  def read(self, value):
    print("read {0}".format(value))

  def write(self, value):
    print("write {0}".format(value))

def test_extract_points(header, points, now=None):
  if now is None:
    now = int(time.time())
  archives = iter(header['archives'])
  currentArchive = next(archives)
  currentPoints = []
  fh = DummyFileHandle()

  for point in points:
    age = now - point[0]

    while currentArchive['retention'] < age:  # We can't fit any more points in this archive
      if currentPoints:  # Commit all the points we've found that it can fit
        currentPoints.reverse()  # Put points in chronological order
        __archive_update_many(fh, header, currentArchive, currentPoints)
        currentPoints = []
      try:
        currentArchive = next(archives)
      except StopIteration:
        currentArchive = None
        break

    if not currentArchive:
      break  # Drop remaining points that don't fit in the database

    currentPoints.append(point)

  # Don't forget to commit after we've checked all the archives
  if currentArchive and currentPoints:
    currentPoints.reverse()
    __archive_update_many(fh, header, currentArchive, currentPoints)

def __archive_update_many(fh, header, archive, points):
  print('__archive_update_many points=%s' % points)
  step = archive['secondsPerPoint']
  alignedPoints = [(timestamp - (timestamp % step), value)
                   for (timestamp, value) in points]
  # Create a packed string for each contiguous sequence of points
  packedStrings = []
  previousInterval = None
  currentString = b""
  lenAlignedPoints = len(alignedPoints)
  for i in xrange(0, lenAlignedPoints):
    # Take last point in run of points with duplicate intervals
    if i + 1 < lenAlignedPoints and alignedPoints[i][0] == alignedPoints[i + 1][0]:
      continue
    (interval, value) = alignedPoints[i]
    if (not previousInterval) or (interval == previousInterval + step):
      currentString += struct.pack(pointFormat, interval, value)
      previousInterval = interval
    else:
      numberOfPoints = len(currentString) // pointSize
      startInterval = previousInterval - (step * (numberOfPoints - 1))
      packedStrings.append((startInterval, currentString))
      currentString = struct.pack(pointFormat, interval, value)
      previousInterval = interval
  if currentString:
    numberOfPoints = len(currentString) // pointSize
    startInterval = previousInterval - (step * (numberOfPoints - 1))
    packedStrings.append((startInterval, currentString))

  # Read base point and determine where our writes will start
  fh.seek(archive['offset'])
  packedBasePoint = fh.read(pointSize)
  (baseInterval, baseValue) = struct.unpack(pointFormat, packedBasePoint)
  if baseInterval == 0:  # This file's first update
    baseInterval = packedStrings[0][0]  # Use our first string as the base, so we start at the start

  # Write all of our packed strings in locations determined by the baseInterval
  for (interval, packedString) in packedStrings:
    timeDistance = interval - baseInterval
    pointDistance = timeDistance // step
    byteDistance = pointDistance * pointSize
    myOffset = archive['offset'] + (byteDistance % archive['size'])
    fh.seek(myOffset)
    archiveEnd = archive['offset'] + archive['size']
    bytesBeyond = (myOffset + len(packedString)) - archiveEnd

    if bytesBeyond > 0:
      fh.write(packedString[:-bytesBeyond])
      assert fh.tell() == archiveEnd, (
        "archiveEnd=%d fh.tell=%d bytesBeyond=%d len(packedString)=%d" %
        (archiveEnd, fh.tell(), bytesBeyond, len(packedString))
      )
      fh.seek(archive['offset'])
      # Safe because it can't exceed the archive (retention checking logic above)
      fh.write(packedString[-bytesBeyond:])
    else:
      fh.write(packedString)

  # Now we propagate the updates to lower-precision archives
  higher = archive
  lowerArchives = [arc for arc in header['archives']
                   if arc['secondsPerPoint'] > archive['secondsPerPoint']]

  for lower in lowerArchives:
    def fit(i):
      return i - (i % lower['secondsPerPoint'])
    lowerIntervals = [fit(p[0]) for p in alignedPoints]
    uniqueLowerIntervals = set(lowerIntervals)
    propagateFurther = False
    for interval in uniqueLowerIntervals:
      if __propagate(fh, header, interval, higher, lower):
        propagateFurther = True

    if not propagateFurther:
      break
    higher = lower

def to_timestamp(str_time):
  return int(time.mktime(time.strptime(str_time, '%Y-%m-%d %H:%M:%S')))

header = {
  'archives': [
    { 'offset': 52, 'secondsPerPoint': 1, 'points': 5 },
    { 'offset': 112, 'secondsPerPoint': 5, 'points': 4 },
    { 'offset': 160, 'secondsPerPoint': 20, 'points': 3 }
  ]
}

pointFormat = "!Ld"
pointSize = struct.calcsize(pointFormat)

for a in header['archives']:
  a['retention'] = a['secondsPerPoint'] * a['points']
  a['size'] = a['points'] * pointSize

print('archives=%s' % header['archives'])

points = [
  (to_timestamp('2018-03-12 16:51:45'), 3),
  (to_timestamp('2018-03-12 16:51:44'), 2),
  (to_timestamp('2018-03-12 16:51:43'), 5),
  (to_timestamp('2018-03-12 16:51:42'), 4),
  (to_timestamp('2018-03-12 16:51:41'), 7),
  (to_timestamp('2018-03-12 16:51:40'), 6),
]
now = to_timestamp('2018-03-12 16:51:45')
test_extract_points(header, points, now)
