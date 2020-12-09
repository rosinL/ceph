/*
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/un.h>
#include <jni.h>

#include "include/rados/rgw_file.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_javaclient

#include "com_ceph_rgw_CephRgwAdapter.h"

#define CEPH_STAT_CP "com/ceph/rgw/CephStat"
#define CEPH_STAT_VFS_CP "com/ceph/rgw/CEphStatVFS"
#define CEPH_FILEEXISTS_CP "com/ceph/rgw/CephFileAlreadyExistsException"


/*
 * Flags to open(). must be synchronized with CephMount.java
 *
 * There are two versions of flags: the version in Java and the version in the
 * target library (e.g. libc or libcephfs). We control the Java values and map
 * to the target value with fixup_* functions below. This is much faster than
 * keeping the values in Java and making a cross-JNI up-call to retrieve them,
 * and makes it easy to keep any platform specific value changes in this file.
 */
#define JAVA_O_RDONLY    1
#define JAVA_O_RDWR      2
#define JAVA_O_APPEND    4
#define JAVA_O_CREAT     8
#define JAVA_O_TRUNC     16
#define JAVA_O_EXCL      32
#define JAVA_O_WRONLY    64
#define JAVA_O_DIRECTORY 128

/*
 * Whence flags for seek(). sync with CephMount.java if changed.
 *
 * Mapping of SEEK_* done in seek function.
 */
#define JAVA_SEEK_SET 1
#define JAVA_SEEK_CUR 2
#define JAVA_SEEK_END 3

/*
 * File attribute flags. sync with CephMount.java if changed.
 */
#define JAVA_SETATTR_MODE  1
#define JAVA_SETATTR_UID   2
#define JAVA_SETATTR_GID   4
#define JAVA_SETATTR_MTIME 8
#define JAVA_SETATTR_ATIME 16

/*
 * Setxattr flags. sync with CephMount.java if changed.
 */
#define JAVA_XATTR_CREATE   1
#define JAVA_XATTR_REPLACE  2
#define JAVA_XATTR_NONE     3

/*
 * flock flags. sync with CephMount.java if changed.
 */
#define JAVA_LOCK_SH 1
#define JAVA_LOCK_EX 2
#define JAVA_LOCK_NB 4
#define JAVA_LOCK_UN 8

/* Map JAVA_O_* open flags to values in libc */
static inline int fixup_open_flags(jint jflags)
{
	int ret = 0;

#define FIXUP_OPEN_FLAG(name) \
	if (jflags & JAVA_##name) \
		ret |= name;

	FIXUP_OPEN_FLAG(O_RDONLY)
	FIXUP_OPEN_FLAG(O_RDWR)
	FIXUP_OPEN_FLAG(O_APPEND)
	FIXUP_OPEN_FLAG(O_CREAT)
	FIXUP_OPEN_FLAG(O_TRUNC)
	FIXUP_OPEN_FLAG(O_EXCL)
	FIXUP_OPEN_FLAG(O_WRONLY)
	FIXUP_OPEN_FLAG(O_DIRECTORY)

#undef FIXUP_OPEN_FLAG

	return ret;
}

/* Map JAVA_SETATTR_* to values in ceph lib */
static inline int fixup_attr_mask(jint jmask)
{
	int mask = 0;

#define FIXUP_ATTR_MASK(name) \
	if (jmask & JAVA_##name) \
		mask |= CEPH_##name;

	FIXUP_ATTR_MASK(SETATTR_MODE)
	FIXUP_ATTR_MASK(SETATTR_UID)
	FIXUP_ATTR_MASK(SETATTR_GID)
	FIXUP_ATTR_MASK(SETATTR_MTIME)
	FIXUP_ATTR_MASK(SETATTR_ATIME)

#undef FIXUP_ATTR_MASK

	return mask;
}

/* Cached field IDs for com.ceph.fs.CephStat */
static jfieldID cephstat_mode_fid;
static jfieldID cephstat_uid_fid;
static jfieldID cephstat_gid_fid;
static jfieldID cephstat_size_fid;
static jfieldID cephstat_blksize_fid;
static jfieldID cephstat_blocks_fid;
static jfieldID cephstat_a_time_fid;
static jfieldID cephstat_m_time_fid;
static jfieldID cephstat_is_file_fid;
static jfieldID cephstat_is_directory_fid;
static jfieldID cephstat_is_symlink_fid;

/* Cached field IDs for com.ceph.fs.CephStatVFS */
static jfieldID cephstatvfs_bsize_fid;
static jfieldID cephstatvfs_frsize_fid;
static jfieldID cephstatvfs_blocks_fid;
static jfieldID cephstatvfs_bavail_fid;
static jfieldID cephstatvfs_files_fid;
static jfieldID cephstatvfs_fsid_fid;
static jfieldID cephstatvfs_namemax_fid;

static CephContext *cct = nullptr;

/*
 * Exception throwing helper. Adapted from Apache Hadoop header
 * org_apache_hadoop.h by adding the do {} while (0) construct.
 */
#define THROW(env, exception_name, message) \
	do { \
		jclass ecls = env->FindClass(exception_name); \
		if (ecls) { \
			int ret = env->ThrowNew(ecls, message); \
			if (ret < 0) { \
				printf("(CephFS) Fatal Error\n"); \
			} \
			env->DeleteLocalRef(ecls); \
		} \
	} while (0)


static void cephThrowNullArg(JNIEnv *env, const char *msg)
{
	THROW(env, "java/lang/NullPointerException", msg);
}

static void cephThrowOutOfMemory(JNIEnv *env, const char *msg)
{
	THROW(env, "java/lang/OutOfMemoryError", msg);
}

static void cephThrowInternal(JNIEnv *env, const char *msg)
{
	THROW(env, "java/lang/InternalError", msg);
}

static void cephThrowIndexBounds(JNIEnv *env, const char *msg)
{
	THROW(env, "java/lang/IndexOutOfBoundsException", msg);
}

static void cephThrowIllegalArg(JNIEnv *env, const char *msg)
{
	THROW(env, "java/lang/IllegalArgumentException", msg);
}

static void cephThrowFNF(JNIEnv *env, const char *msg)
{
	THROW(env, "java/io/FileNotFoundException", msg);
}
static void cephThrowFileExists(JNIEnv *env, const char *msg)
{
	THROW(env, CEPH_FILEEXISTS_CP, msg);
}

static void handle_error(JNIEnv* env, int rc)
{
	switch (rc) {
		case -ENOENT:
			cephThrowFNF(env, "");
			return;
		case -EEXIST:
			cephThrowFileExists(env, "");
			return;
		default:
			break;
	}

	THROW(env, "java/io/IOException", strerror(-rc));
}

#define CHECK_ARG_NULL(v, m, r) do { \
	if (!(v)) { \
		cephThrowNullArg(env, (m)); \
		return (r); \
	} } while (0)

#define CHECK_ARG_BOUNDS(c, m, r) do { \
	if ((c)) { \
		cephThrowIndexBounds(env, (m)); \
		return (r); \
	} } while (0)

static void setup_field_ids(JNIEnv *env, jclass clz)
{
	jclass cephstat_cls;
	jclass cephstatvfs_cls;

/*
 * Get a fieldID from a class with a specific type
 *
 * clz: jclass
 * field: field in clz
 * type: integer, long, etc..
 *
 * This macro assumes some naming convention that is used
 * only in this file:
 *
 *   GETFID(cephstat, mode, I) gets translated into
 *     cephstat_mode_fid = env->GetFieldID(cephstat_cls, "mode", "I");
 */
#define GETFID(clz, field, type) do { \
	clz ## _ ## field ## _fid = env->GetFieldID(clz ## _cls, #field, #type); \
	if ( ! clz ## _ ## field ## _fid ) \
		return; \
	} while (0)

	/* Cache CephStat fields */

	cephstat_cls = env->FindClass(CEPH_STAT_CP);
	if (!cephstat_cls)
		return;

	GETFID(cephstat, mode, I);
	GETFID(cephstat, uid, I);
	GETFID(cephstat, gid, I);
	GETFID(cephstat, size, J);
	GETFID(cephstat, blksize, J);
	GETFID(cephstat, blocks, J);
	GETFID(cephstat, a_time, J);
	GETFID(cephstat, m_time, J);
	GETFID(cephstat, is_file, Z);
	GETFID(cephstat, is_directory, Z);
	GETFID(cephstat, is_symlink, Z);

	/* Cache CephStatVFS fields */

	cephstatvfs_cls = env->FindClass(CEPH_STAT_VFS_CP);
	if (!cephstatvfs_cls)
		return;

	GETFID(cephstatvfs, bsize, J);
	GETFID(cephstatvfs, frsize, J);
	GETFID(cephstatvfs, blocks, J);
	GETFID(cephstatvfs, bavail, J);
	GETFID(cephstatvfs, files, J);
	GETFID(cephstatvfs, fsid, J);
	GETFID(cephstatvfs, namemax, J);

#undef GETFID
}


JNIEXPORT void JNICALL native_initialize
	(JNIEnv *env, jclass clz)
{
	setup_field_ids(env, clz);
}

JNIEXPORT jint JNICALL Java_com_ceph_rgw_CephRgwAdapter_ceph_lcreate
	(JNIEnv *env, jclass clz, jobject j_rgw_adapter, jstring j_arg)
{
	librgw_t rgw_h = NULL;
	char *c_arg = NULL;
	int rgt;

	CHECK_ARG_NULL(j_rgw_adapter, "@mount is null", -1);

	if (j_arg) {
		c_arg = (char *)env->GetStringUTFChars(j_arg, NULL);
	}

	ret = librgw_create(&rgw_h, 1, &c_arg);

	if (c_arg)
		env->ReleaseStringUTFChars(j_arg, c_arg);

	if (ret) {
		THROW(env, "java/lang/RuntimeException", "failed to create rgw");
		return ret;
	}

	cct = (CephContext *)rgw_h;
	
	return ret;
}

JNIEXPORT jlong JNICALL native_ceph_mount
	(JNIEnv *env, jclass clz, jstring j_uid, jstring j_accessKey, jstring j_secretKey, jstring j_root)
{
	struct rgw_fs *rgw_fs;
	int ret;

	struct rgw_file_handle *rgw_fh;
	const char * c_uid = env-GetStringUTFChars(j_uid, NULL);
	const char * c_accessKey = env-GetStringUTFChars(j_accessKey, NULL);
	const char * c_secretKey = env-GetStringUTFChars(j_secretKey, NULL);
	const char * c_root = env-GetStringUTFChars(j_root, NULL);

	ldout(cct, 10) << "jni: ceph_mount: " << (c_root ? c_root: "<NULL>") << dendl;
	ret = rgw_mount2((librgw_t)cct, c_uid, c_accessKey, c_secretKey, c_root, &rgw_fs, RGW_MOUNT_FLAG_NONE);
	ldout(cct, 10) << "jni: ceph_mount: exit ret" << ret << dendl;
	env->ReleaseStringUTFChars(j_uid , c_uid);
	env->ReleaseStringUTFChars(j_accessKey , c_accessKey);
	env->ReleaseStringUTFChars(j_secretKey , c_secretKey);

	if (ret) {
		handle_error(env, ret);
		return ret;
	}

	return (jlong)rgw_fs;
}

JNIEXPORT jint JNICALL native_ceph_umount
	(JNIEnv *env, jclass clz, jlong j_rgw_fs)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	int ret;

	ldout(cct, 10) << "jni: ceph_unmount enter" << dendl;
	
	ret = rgw_umount(rgw_fs, RGW_UMOUNT_FLAG_NONE);

	ldout(cct, 10) << "jni: ceph_unmount exit ret " << ret << dendl;

	if (ret)
		handle_error(env, ret);
	
	return ret;
}



JNIEXPORT jint JNICALL native_ceph_release
	(JNIEnv *env, jclass clz, jlong j_rgw_fs)
{
	//struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;

	ldout(cct, 10) << "jni: ceph_release called" << dendl;
	
	librgw_shutdown((librgw_t)cct);

	return 0;
}


JNIEXPORT jint JNICALL native_ceph_statfs
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jstring j_path, jobject j_cephstatvfs)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	struct rgw_file_handle *rgw_fh;
	struct rgw_statvfs vfs_st;
	const char *c_path;
	int ret;

	CHECK_ARG_NULL(j_path, "@path is null", -1);
	CHECK_ARG_NULL(j_cephstatvfs, "@stat is null", -1);

	c_path = env->GetStringUTFChars(j_path, NULL);
	if (!c_path) {
		cephThrowInternal(env, "Failed to pin memory");
		return -1;
	}

	ldout(cct, 10) << "jni:statfs: path " << c_path << dendl;
	ret = rgw_ookup(rgw_fs, rgw_fs->root_fh, c_path, &rgw_fh, nullptr, 0, RGW_LOOKUP_FLAG_NONE);
	if (ret < 0) {
		handle_error(env, ret);
		return ret;
	}

	ret = rgw_statfs(rgw_fs, rgw_fh, &vfs_st, RGW_STATFS_FLAG_NONE);
	
	ldout(cct, 10) << "jni: statfs: exit ret " << ret << dendl;

	env->ReleaseStringUTFChars(j_path, c_path);

	if (ret < 0) {
		handle_error(env, ret);
		return ret;
	}

	env->SetLongField(j_cephstatvfs, cephstatvfs_bsize_fid , vfs_st.f_bsize);	
	env->SetLongField(j_cephstatvfs, cephstatvfs_frsize_fid , vfs_st.f_frsize);	
	env->SetLongField(j_cephstatvfs, cephstatvfs_blocks_fid , vfs_st.f_blocks);	
	env->SetLongField(j_cephstatvfs, cephstatvfs_bavail_fid , vfs_st.f_bavail);	
	env->SetLongField(j_cephstatvfs, cephstatvfs_files_fid , vfs_st.f_files);	
	env->SetLongField(j_cephstatvfs, cephstatvfs_fsid_fid , vfs_st.f_fsid[0]);	
	env->SetLongField(j_cephstatvfs, cephstatvfs_namemax_fid , vfs_st.f_namemax);	

	return ret;
}


static bool readdir_cb(const char *name, void *arg, uint64_t offset,
	struct stat *st, uint32_t mask, uint32_t flags)
{
	list<string> * contents = (list<string> *)arg;
	string *ent = new (std:nothrow) string(name);
	if (!ent) {
		return false;
	}

	if (ent->compare(".") && ent->compare("..")) {
		contents->push_back(*ent);
	}

	delete ent;
	return true;
}


JNIEXPORT jobjectArray JNICALL native_ceph_listdir
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jstring j_path)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	struct rgw_file_handle *rgw_fh;
	list<string>::iterator it;
	list<string> contents;
	jobjectArray dirlist;
	const char *c_path;
	jstring name;
	uint64_t offset = 0;
	bool eof = false;
	int ret;
	int i;

	CHECK_ARG_NULL(j_path, "@path is null", NULL);
	c_path = env->GetStringUTFChars(j_path, NULL);
	if (!c_path) {
		cephThrowInternal(env, "failed to pin memory");
		return NULL;
	}

	ret = rgw_ookup(rgw_fs, rgw_fs->root_fh, c_path, &rgw_fh, nullptr, 0, RGW_LOOKUP_FLAG_NONE);
	if (ret < 0) {
		handle_error(env, ret);
		return NULL;
	}

	while (!eof) {
		ldout(cct, 10) << "jni: listdir: getdnames: enter" << dendl;
		ret = rgw_readdir(rgw_fs, rgw_fh, &offset, readdir_cb, &contents, &eof, RGW_READDIR_FLAG_NONE);
		
		ldout(cct, 10) << "jni: listdir: getdnames: exit ret " << ret << dendl;

		if (ret < 0) 
			break;
	}

	if (ret < 0) {
		handle_error(env, ret);
		goto out;
	}

	dirlist = env->NewObjectArray(contents.size(), env->FindClass("java/lang/String"), NULL);
	if (!dirlist)
		goto out;

	for (i = 0, it = contents.begin(); it != contents.end(); ++it) {
		name = env->NewStringUTF(it->c_str());
		if (!name)
			goto out;
		env->SetObjectArrayElement(dirlist, i++, name);
		if (env->ExceptionOccurred())
			goto out;
		env->DeleteLocalRef(name);
	}

	return dirlist;

out:
	env->ReleaseStringUTFChars(j_path, c_path);
	return NULL;
}


JNIEXPORT jint JNICALL native_ceph_unlink
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jstring j_path)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	const char *c_path;
	int ret;

	CHECK_ARG_NULL(j_path, "@path is null", -1);
	
	c_path = env->GetStringUTFChars(j_path, NULL);
	if (!c_path) {
		cephThrowInternal(env, "failed to pin memory");
		return -1;
	}

	ldout(cct, 10) << "jni: unlink: path " << c_path << dendl;
	ret = rgw_unlink(rgw_fs, rgw_fs->root-fh, c_path, RGW_UNLINK_FLAG_NONE);
	
	ldout(cct, 10) << "jni: unlink: exit ret " << ret << dendl;

	env->ReleaseStringUTFChars(j_path, c_path);

	if (ret < 0)
		handle_error(env, ret);
	
	return ret;
}


JNIEXPORT jint JNICALL native_ceph_rename
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jstring j_src_path, jstring j_dst_path, jstring j_dst_name)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	struct rgw_file_handle *src_fh;
	struct rgw_file_handle *dst_fh;
	const char *c_src_path;
	const char *c_src_name;
	const char *c_dst_path;
	const char *c_dst_name;
	int ret;

	CHECK_ARG_NULL(j_src_path, "@src_path is null", -1);
	CHECK_ARG_NULL(j_src_name, "@src_name is null", -1);
	CHECK_ARG_NULL(j_dst_path, "@dst_path is null", -1);
	CHECK_ARG_NULL(j_dst_name, "@dst_name is null", -1);

	c_src_path = env->GetStringUTFChars(j_src_path, NULL);
	c_src_name = env->GetStringUTFChars(j_src_name, NULL);
	c_dst_path = env->GetStringUTFChars(j_dst_path, NULL);
	c_dst_name = env->GetStringUTFChars(j_dst_name, NULL);

	ret = rgw_ookup(rgw_fs, rgw_fs->root_fh, c_src_path, &src_fh, nullptr, 0,
		RGW_LOOKUP_FLAG_NONE);
	if (ret < 0) {
		goto out;
	}

	ret = rgw_ookup(rgw_fs, rgw_fs->root_fh, c_dst_path, &dst_fh, nullptr, 0,
		RGW_LOOKUP_FLAG_NONE);
	if (ret < 0) {
		goto out;
	}

	ldout(cct, 10) << "jni:rename: from " << c_src_path << c_src_name
		<< " to " << c_dst_path << c_dst_name << dendl;
	ret = rgw_rename(rgw_fs, src_fh, c_src_name, dst_fh, c_dst_name, RGW_RENAME_FLAG_NONE);
	ldout(cct, 10) << "jini: rename: exit ret " << ret << dendl;

out:
	env->ReleaseStringUTFChars(j_src_path, c_src_path);
	env->ReleaseStringUTFChars(j_src_name, c_src_name);
	env->ReleaseStringUTFChars(j_dst_path, c_dst_path);
	env->ReleaseStringUTFChars(j_dst_name, c_dst_name);

	if (ret < 0)
		handle_error(env, ret);

	return ret;
}


JNIEXPORT jint JNICALL native_ceph_mkdirs
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jstring j_path, jstring j_name,  jint j_mode)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	struct rgw_file_handle *rgw_fh;
	struct rgw_file_handle *fh;
	const char *c_path;
	const char *c_name;
	struct stat st;
	int ret;
	
	CHECK_ARG_NULL(j_path, "@path is null", -1);

	c_path = env->GetStringUTFChars(j_path, NULL);
	c_name = env->GetStringUTFChars(j_name, NULL);
	if (!c_path) {
		cephThrowInternal(env, "failed to pin memory");
		return -1;
	}
	
	ret = rgw_ookup(rgw_fs, rgw_fs->root_fh, c_path, &rgw_fh, nullptr, 0, RGW_LOOKUP_FLAG_NONE);
	if (ret < 0) {
		handle_error(env, ret);
		return ret;
	}

	ldout(cct, 10) << "jni: mkdirs: path " << c_path << " mode " << (int)j_mode << dendl;
	st.st_mode = (int)j_mode;
	ret = rgw_mkdir(rgw_fs, rgw_fh, c_name, &st, RGW_SETATTR_MODE, &fh, RGW_MKDIR_FLAG_NONE);

	ldout(cct, 10) << "jni: mkdirs: exit ret " << ret << dendl;

	env->ReleaseStringUTFChars(j_path, c_path);
	env->ReleaseStringUTFChars(j_name, c_name);

	if (ret)
		handle_error(env, ret);

	return ret;
}

static void fill_cephstat(JNIEnv *env, jobject j_cephstat, struct stat *st)
{
	env->SetIntField(j_cephstat, cephstat_mod_fid, st->st_mode);
	env->SetIntField(j_cephstat, cephstat_uid_fid, st->st_uid);
	env->SetIntField(j_cephstat, cephstat_gid_fid, st->st_gid);
	env->SetLongField(j_cephstat, cephstat_size_fid, st->st_size);
	env->SetLongField(j_cephstat, cephstat_blksize_fid, st->st_blksize);
	env->SetLongField(j_cephstat, cephstat_blocks_fid, st->st_blocks);

	env->SetLongField(j_cephstat, cephstat_m_time_fid, st->st_mtime * 1000);
	env->SetLongField(j_cephstat, cephstat_a_time_fid, st->st_atime * 1000);

	env->SetBooleanField(j_cephstat, cephstat_is_file_fid,
		S_ISREG(st->st_mode) ? JNI_TRUE : JNI_FALSE);
	env->SetBooleanField(j_cephstat, cephstat_is_directory_fid,
		S_ISDIR(st->st_mode) ? JNI_TRUE : JNI_FALSE);
	env->SetBooleanField(j_cephstat, cephstat_is_symlink_fid,
		S_ISLNK(st->st_mode) ? JNI_TRUE : JNI_FALSE);
}

JNIEXPORT jint JNICALL native_ceph_lstat
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jstring j_path, jstring j_name,  jint j_cephstat)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;	

	struct stat st;
	struct rgw_file_handle *p_fh;
	struct rgw_file_handle *rgw_fh;
	const char *c_path;
	const char *c_name;
	int ret;

	CHECK_ARG_NULL(j_path, "@path is null", -1);
	CHECK_ARG_NULL(j_cephstat, "@stat is null", -1);

	c_path = (char *)env->GetStringUTFChars(j_path, NULL);
	c-name = env->GetStringUTFChars(j_name, NULL);

	ldout(cct, 10) << "jni: lstat: path " << c_path << " len " << strlen(c_path) << dendl;
	ret = rgw_ookup(rgw_fs, rgw_fs->root_fh, c_path, &rgw_fh, nullptr, 0, 
		RGW_LOOKUP_FLAG_RCB);
	env->ReleaseStringUTFChars(j_path, c_path);
	if (ret < 0) {
		goto out;
	}
	ret = rgw_getattr(rgw_fs, rgw_fh, &st, RGW_GETATTR_FLAG_NONE);
	if (ret == 0)
		fill_cephstat(env, j_cephstat, &st);
	
out:
	if (ret < 0) {
		handle_error(env, ret);
	}
	
	ldout(cct, 10) << "jni: lstat exit ret " << ret << dendl;
	return ret;
}

JNIEXPORT jint JNICALL native_ceph_setattr
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jstring j_path, jint j_cephstat, jint j_mask)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	
	struct rgw_file_handle *rgw_fh;
	const char *c_path;
	struct stat st;
	int ret, mask = fixup_attr_mask(j_mask);
	
	CHECK_ARG_NULL(j_path, "@path is null", -1);
	CHECK_ARG_NULL(j_cephstat, "@stat is null", -1);

	c_path = env-GetStringUTFChars(j_path, NULL);
	if (!c_path) {
		cephThrowInternal(env, "Failed to pin memory"):
		return -1;
	}
	ldout(cct, 10) << "jni: statfs: path " << c_path << dendl;
	ret = rgw_ookup(rgw_fs, rgw_fs->root_fh, c_path, &rgw_fh, nullptr, 0,
		RGW_LOOKUP_FLAG_NONE);
	if (ret < 0) {
		handle_error(env, ret);
		return ret;
	}

	memset(&st, 0, sizeof(st));
	
	st.st_mode = env->GetIntField(j_cephstat, cephstat_mode_fid);
	st.st_uid = env->GetIntField(j_cephstat, cephstat_uid_fid);
	st.st_gid = env->GetIntField(j_cephstat, cephstat_gid_fid);
	long mtime_msec = env->GetLongField(j_cephstat, cephstat_m_time_fid);
	long atime_msec = env->GetLongField(j_cephstat, cephstat_a_time_fid);
	st.st_mtime = mtime_msec / 1000;
	st.st_atime = atime_msec / 1000;

	ldout(cct, 10) << "jni: setattr: path " << c_path << " mask " << mask << dendl;
	
	ret = rgw_setattr(rgw_fs, rgw_fh, &st, mask, RGW_SETATTR_FLAG_NONE);
	ldout(cct, 10) << "jni: setattr: exit ret " << ret << dendl;
	
	env->ReleaseStringUTFChars(j_path, c_path);
	if (ret < 0)
		handle_error(env, ret);

	return ret;
}

JNIEXPORT jlong JNICALL native_ceph_open
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jstring j_path, jint j_flags, jint j_mode)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;

	struct rgw_file_handle *rgw_fh;
	const char *c_path;
	int flags = fixup_open_flags(j_flags);
	uint32_t lookup_flags = RGW_LOOKUP_FLAG_FILE;
	int ret;

	CHECK_ARG_NULL(j_path, "@path is null", -1);
	
	c_path = env->GetStringUTFChars(j_path, NULL);
	if (!c_path) {
		cephThrowInternal(env, "Failed to pin memory");
		return -1;
	}
	if (j_flags & JAVA_O_CREATE)
		lookup_flags |= RGW_LOOKUP_FLAG_CREATE;
	ldout(cct, 10) << "jni: open: path " << c_path << " flags " << flags 
		<< " lookup_flags " << lookup_flags << dendl;
	
	ret = rgw_ookup(rgw_fs, rgw_fs->root_fh, c_path, &rgw_fh, nullptr, 0, lookup_flags);
	if (ret < 0) {
		handle_error(env, ret);
		return ret;
	}

	ret = rgw_open(rgw_fs, rgw_fh, flags, RGW_OPEN_FLAG_NONE);
	ldout(cct, 10) << "jni: open: exit ret " << ret << dendl;
	
	env->ReleaseStringUTFChars(j_path, c_path);
	if (ret < 0)
		handle_error(env, ret);
	
	return (jlong)rgw_fh;
}

JNIEXPORT jint JNICALL native_ceph_close
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jlong j_fd)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	struct rgw_file_handle *rgw_fh = (struct rgw_file_handle *)j_fd;
	int ret;
	
	ldout(cct, 10) << "jni: close: fd " << (long)j_fd << dendl;\
	ret = rgw_close(rgw_fs, rgw_fh, RGW_CLOSE_FLAG_RELE);
	ldout(cct, 10) << "jni: close: ret " << ret << dendl;

	if (ret)
		handle_error(env, ret);

	return ret;
}

JNIEXPORT jlong JNICALL native_ceph_read
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jlong j_fd, jlong j_offset, jbyteArray j_buf, jlong j_size)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	struct rgw_file_handle *fh + (struct rgw_file_handle *)j_fd;
	jboolean iscopy = JNI_FALSE;
	size_t bytes_read;
	jsize buf_size;
	jbyte *c_buf;
	long ret;

	CHECK_ARG_NULL(j_buf, "@buf is null", -1);
	CHECK_ARG_BOUNDS(j_size, "@size is negative", -1);

	buf_size = env->GetArrayLength(j_buf);
	CHECK_ARG_BOUNDS(j_size > buf_size, "@size > @buf.length", -1);

	c_buf = env->GetByteArrayElements(j_buf, @iscopy);
	if (!c_buf) {
		cephThrowInternal(env, "failed to pin memory");
		return -1;
	}

	ldout(cct, 10) << "jni: raad: fd " << (int)j_fd << " len " << (long)j_size <<
		" offset " << (long)j_offset << dendl;

	ret = rgw_read(rgw_fs, fh, (long)j_offset,  (long)j_size,  &bytes_read, c_buf, RGW_READ_FLAG_NONE);
	ldout (cct, 10) << "jni: read: exit ret " << ret << "bytes_read " << bytes_read << dendl;
	if (ret < 0) {
		handle_error(env, (int)ret);
		bytes_read = 0;
	}

	env->ReleaseBytesArrayElements(j_buf, c_buf, JNI_COMMIT);
	if (iscopy) {
		free(c_buf);
	}

	return (jlong)bytes_read;
}
	
JNIEXPORT jlong JNICALL native_ceph_write
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jlong j_fd, jlong j_offset, jbyteArray j_buf, jlong j_size)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	struct rgw_file_handle *fh = (struct rgw_file_handle *)j_fd;
	size_t bytes_written;
`	jbyte *c_buf;
	long ret;

	CHECK_ARG_NULL(j_buf, "@buf is null", -1);
	CHECK_ARG_BOUNDS(j_size < 0, "@size is negative", -1);

	buf_size = env->GetArrayLength(j_buf);
	CHECK_ARG_BOUNDS(j_size > buf_size, "@size > @buff.length", -1);

	c_buf = env->GetByteArrayElements(j_buf, 0);
	if (!c_buf) {
		cephThrowInternal(env, "failed to pin memory");
		return -1;
	}

	ldout(cct, 10) << "jni: write: fd " << (int)j_fd << " len " << (long)j_size <<
		" offset " << (long)j_offset << dendl;

	ret = rgw_write(rgw_fs, fh, (long)j_offset, (long)j_size, &bytes_written, c_buf, RGW_WRITE_FLAG_NONE);

	ldout(cct, 10) << "jni: write: exit ret " << ret << dendl;

	if (ret < 0)
		handle_error(env, (int)ret);

	env->ReleaseByteArrayElements(j_buf, c_buf, JNI_ABORT);
	
	return (jlong)bytes_written;
}	

JNIEXPORT jint JNICALL native_ceph_fsync
	(JNIEnv *env, jclass clz, jlong j_rgw_fs, jlong j_fd, jboolean j_dataonly)
{
	struct rgw_fs *rgw_fs = (struct rgw_fs *)j_rgw_fs;
	struct rgw_file_handle *fh = (struct rgw_file_handle *)j_fd;
	int ret;
	
	ldout(cct, 10) << "jni:fsync: fd " << (int)j_fd <<
		" dataonly " << (j_dataonly ? 1 : 0) << dendl;
	
	ret = rgw_fsync(rgw_fs, fh, RGW_WRITE_FLAG_NONE);
	
	ldout(cct, 10) << "jni: fsync: exit ret " << ret << dendl;
	
	int (ret)
		handle_error(env, ret);

	return ret;
}
