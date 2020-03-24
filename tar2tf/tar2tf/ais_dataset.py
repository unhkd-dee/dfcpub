import os, io, tarfile

import tensorflow as tf

from .aisapi import AisClient
from .ops import Select, SelectJSON, Decode, Func, Resize, Convert

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"

OPS = [Select, SelectJSON, Decode, Func, Resize, Convert]


def __bytes_feature(value):
    """Returns a bytes_list from a string / byte."""
    if isinstance(value, type(tf.constant(0))):
        value = value.numpy()
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))


def __int64_feature(value):
    """Returns an int64_list from a bool / enum / int / uint."""
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))


# pylint: disable=unused-variable
def default_record_parser(record):
    keys_to_features = __default_image_desc()
    parsed = tf.io.parse_single_example(record, keys_to_features)

    img = tf.image.decode_jpeg(parsed["image_raw"])
    img = tf.cast(img, tf.float32)
    img = tf.image.resize(img, (224, 224))
    img = tf.reshape(img, shape=[224, 224, 3])
    label = tf.cast(parsed["label"], tf.int32)

    return img, label


def is_valid_tar_entry(key, ext):
    if ext not in ["jpg", "cls", "json", "png", "jpeg", "bmp"] and not (str.startswith("lab") or str.startswith("cls" or str)):
        return False

    # garbage generated by macos
    if os.path.basename(key).startswith("._"):
        return False

    return True


def __default_image_desc():
    return {
        "height": tf.io.FixedLenFeature([], tf.int64),
        "width": tf.io.FixedLenFeature([], tf.int64),
        "depth": tf.io.FixedLenFeature([], tf.int64),
        "label": tf.io.FixedLenFeature([], tf.int64),
        "image_raw": tf.io.FixedLenFeature([], tf.string),
    }


def tar_records(tar_file):
    records = {}

    while True:
        member_info = tar_file.next()
        if member_info is None:
            break

        member_file = tar_file.extractfile(member_info)
        if member_file is None:
            # memberFile was not a regular file, for instance directory
            continue

        if not member_info.isfile:
            continue

        key, ext = os.path.splitext(member_info.name)
        ext = ext[1:]  # remove leading .

        if not is_valid_tar_entry(key, ext):
            continue

        record = records.get(key, {})
        record["__key__"] = key
        record[ext] = member_file.read()
        records[key] = record

    return records


def default_record_to_pair(record):
    img = tf.image.convert_image_dtype(tf.io.decode_jpeg(record["jpg"]), tf.float32)
    img = tf.image.resize(img, (224, 224))
    return img, record["cls"]


# Create a dictionary with features that may be relevant.
# this is description of serialization for protobuf
def default_record_to_example(record):
    label = int(record["cls"])
    img = tf.image.decode_jpeg(record["jpg"])

    feature_dict = {
        "height": __int64_feature(img.shape[0]),
        "width": __int64_feature(img.shape[1]),
        "depth": __int64_feature(img.shape[2]),
        "label": __int64_feature(label),
        "image_raw": __bytes_feature(record["jpg"]),
    }

    features = tf.train.Features(feature=feature_dict)
    return tf.train.Example(features=features)


def validate_op(op):
    unknown_type_ex = Exception("unknown operation type. Expected one of {}".format(OPS))
    if type(op) == list:
        for o in op:
            if type(o) not in OPS:
                if type(o) == list:
                    raise Exception("list of operations can't contain another list")
                raise unknown_type_ex
    if type(op) not in OPS:
        raise unknown_type_ex


# pylint: disable=unused-variable
class AisDataset:
    def __init__(self, bucket, proxy_url="localhost:8080", val_op=Resize(Convert(Decode("jpg"), tf.float32), (224, 224)), label_op=Select("cls")):
        self.proxy_url = proxy_url
        self.proxy_client = AisClient(self.proxy_url, bucket)
        self.bucket = bucket

        validate_op(val_op)
        validate_op(label_op)

        # how to translate record to value / label
        self.val_op = val_op
        self.label_op = label_op

    def __get_object(self, object_name):
        return self.proxy_client.get_object(object_name)

    def __get_object_names(self, template):
        smap = self.proxy_client.get_cluster_info()

        for k in smap["tmap"]:
            for o in self.proxy_client.get_objects_names(smap["tmap"][k]["intra_data_net"]["direct_url"], template).json():
                yield o

    # args:
    # record_to_pair - specify how to translate tar record to pair
    # path - place to save TFRecord file. If None, everything done on the fly
    def load_from_tar(self, template, **kwargs):
        accepted_args = ["output_types", "output_shapes", "path"]

        for key in kwargs:
            if key not in accepted_args:
                raise Exception("invalid argument name {}".format(key))

        output_types = kwargs.get("output_types", (tf.float32, tf.int32))
        output_shapes = kwargs.get("output_shapes", (tf.TensorShape([224, 224, 3]), tf.TensorShape([])))

        if "path" in kwargs and kwargs["path"] is not None:
            return self.__record_dataset_from_tar(template, kwargs["path"], record_to_example=default_record_to_example)

        return tf.data.Dataset.from_generator(lambda: self.__generator_from_template(template), output_types, output_shapes=output_shapes)

    def __generator_from_template(self, template):
        for obj in self.__get_object_names(template):
            obj = self.__get_object(obj)
            tar_bytes = io.BytesIO(obj)
            tar = tarfile.open(mode="r", fileobj=tar_bytes)
            records = tar_records(tar)
            for k in records:
                val, label = self.operation_to_value(self.val_op, records[k]), self.operation_to_value(self.label_op, records[k])
                yield val, label

            tar.close()

    # path - path where to save record dataset
    def __record_dataset_from_tar(self, template, path, record_to_example):
        with tf.io.TFRecordWriter(path) as writer:
            for tar_name in self.__get_object_names(template):
                tar_obj = self.__get_object(tar_name)
                tar_bytes = io.BytesIO(tar_obj)
                tar = tarfile.open(mode="r", fileobj=tar_bytes)
                records = tar_records(tar)

                for k in records:
                    record = records[k]
                    tf_example = record_to_example(record)

                    writer.write(tf_example.SerializeToString())
                tar.close()

    def operation_to_value(self, op, record, ext_required=False):
        if type(op) == list:
            d = {}
            for o in op:
                d[op.ext_name] = self.operation_to_value(o, record, True)
            return d

        if ext_required and op.ext_name is None:
            raise Exception("required ext_name, but None found")

        return op.do(record)
