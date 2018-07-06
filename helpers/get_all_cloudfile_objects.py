
def get_all_cloudfile_objects(cfBucketName, cfList, cfConn, max_object_count, marker=None):
  objects = cfConn.get_container(cfBucketName).get_objects(limit=max_object_count, marker=marker)
  cfList.extend(objects)

  if len(objects) == max_object_count:
     return get_all_objects(cfBucketName, cfList, cfConn, max_object_count, cfList[-1].name)
  else:
     return cfList
