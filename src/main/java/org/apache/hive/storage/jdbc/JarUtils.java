package org.apache.hive.storage.jdbc;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @className: JarUtils
 * @description: TODO 类描述
 * @author: jiangsd3
 * @date: 2021/5/12
 **/
public class JarUtils {
    private static final Logger log = LoggerFactory.getLogger(JarUtils.class);

    public static void addDependencyJars(Configuration conf, List<Class<?>> classes) throws IOException {
        FileSystem localFs = FileSystem.getLocal(conf);
        Set<String> jars = new HashSet<String>();
        // Add jars that are already in the tmpjars variable
        jars.addAll(conf.getStringCollection("tmpjars"));

        // add jars as we find them to a map of contents jar name so that we can
        // avoid
        // creating new jars for classes that have already been packaged.
        Map<String,String> packagedClasses = new HashMap<String,String>();

        // Add jars containing the specified classes
        for (Class<?> clazz : classes) {
            if (clazz == null) {
                continue;
            }
            Path path = findOrCreateJar(clazz, localFs, packagedClasses);
            if (path == null) {
                log.warn("Could not find jar for class " + clazz + " in order to ship it to the cluster.");
                continue;
            }
            if (!localFs.exists(path)) {
                log.warn("Could not validate jar file " + path + " for class " + clazz);
                continue;
            }
            jars.add(path.toString());
        }
        if (!jars.isEmpty()) {
            conf.set("tmpjars", StringUtils.join(jars, ","));
        }
    }

    /**
     * If org.apache.hadoop.util.JarFinder is available (0.23+ hadoop), finds the Jar for a class or
     * creates it if it doesn't exist. If the class is in a directory in the classpath, it creates a
     * Jar on the fly with the contents of the directory and returns the path to that Jar. If a Jar is
     * created, it is created in the system temporary directory. Otherwise, returns an existing jar
     * that contains a class of the same name. Maintains a mapping from jar contents to the tmp jar
     * created.
     *
     * @param my_class
     *          the class to find.
     * @param fs
     *          the FileSystem with which to qualify the returned path.
     * @param packagedClasses
     *          a map of class name to path.
     * @return a jar file that contains the class.
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    private static Path findOrCreateJar(Class<?> my_class, FileSystem fs,
                                        Map<String,String> packagedClasses) throws IOException {
        // attempt to locate an existing jar for the class.
        String jar = findContainingJar(my_class, packagedClasses);
//        if (StringUtils.isEmpty(jar)) {
//            jar = getJar(my_class);
//            updateMap(jar, packagedClasses);
//        }

        if (StringUtils.isEmpty(jar)) {
            return null;
        }

        log.debug("For class {}, using jar {}", my_class.getName(), jar);
        return new Path(jar).makeQualified(fs);
    }

    private static String findContainingJar(Class<?> my_class, Map<String,String> packagedClasses)
            throws IOException {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";

        // first search the classpath
        for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements();) {
            URL url = itr.nextElement();
            if ("jar".equals(url.getProtocol())) {
                String toReturn = url.getPath();
                if (toReturn.startsWith("file:")) {
                    toReturn = toReturn.substring("file:".length());
                }
                // URLDecoder is a misnamed class, since it actually decodes
                // x-www-form-urlencoded MIME type rather than actual
                // URL encoding (which the file path has). Therefore it would
                // decode +s to ' 's which is incorrect (spaces are actually
                // either unencoded or encoded as "%20"). Replace +s first, so
                // that they are kept sacred during the decoding process.
                toReturn = toReturn.replaceAll("\\+", "%2B");
                toReturn = URLDecoder.decode(toReturn, "UTF-8");
                return toReturn.replaceAll("!.*$", "");
            }
        }

        // now look in any jars we've packaged using JarFinder. Returns null
        // when
        // no jar is found.
        return packagedClasses.get(class_file);
    }
}