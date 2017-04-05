package common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** Distributed filesystem paths.

    <p>
    Objects of type <code>Path</code> are used by all filesystem interfaces.
    Path objects are immutable.

    <p>
    The string representation of paths is a forward-slash-delimeted sequence of
    path components. The root directory is represented as a single forward
    slash.

    <p>
    The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
    not permitted within path components. The forward slash is the delimeter,
    and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Comparable<Path>, Serializable
{
    private static final String DELIMITER = "/";
    private String pathString;

    // Kind of a hack.
    private static java.nio.file.Path stringToPath (String string)
    {
        /** : Not sure. Refer Documentation once again.*/
        return Paths.get(string).normalize();
    }

    /** Creates a new path which represents the root directory. */
    public Path()
    {
        this (DELIMITER);
    }

    /** Creates a new path by appending the given component to an existing path.

        @param path The existing path.
        @param component The new component.
        @throws IllegalArgumentException If <code>component</code> includes the
                                         separator, a colon, or
                                         <code>component</code> is the empty
                                         string.
    */
    public Path(Path path, String component)
    {
        // this has to be used.... But should be first statement in constructor. ToDo.
        java.nio.file.Path newPath = stringToPath(path.pathString).resolve(component);
        String newPathString = newPath.toString();
        if (component.contains("/"))
        {
            // according to the comments;
            throw new IllegalArgumentException("Component contains \'/\'");
        }
        if (component.equals(""))
        {
            // according to the comments;
            throw new IllegalArgumentException("Component given is an empty string");
        }
    }

    /** Creates a new path from a path string.

        <p>
        The string is a sequence of components delimited with forward slashes.
        Empty components are dropped. The string must begin with a forward
        slash.

        @param path The path string.
        @throws IllegalArgumentException If the path string does not begin with
                                         a forward slash, or if the path
                                         contains a colon character.
     */

    private static boolean emptyPath (String path)
    {
        return (path.length() == 0);
    }

    public Path(String path)
    {
        if(emptyPath(path) || (path.charAt(0) != DELIMITER.charAt(0)))
        {
            // according to the comments;
            throw new IllegalArgumentException("Given path does not start with \'/\'");
        }

        if(path.contains(":"))
        {
            // according to the comments;
            throw new IllegalArgumentException("Given path contain \':\'");
        }

        this.pathString = stringToPath(path).toString();

    }

    /** Returns an iterator over the components of the path.

        <p>
        The iterator cannot be used to modify the path object - the
        <code>remove</code> method is not supported.

        @return The iterator.
     */
    @Override
    public Iterator<String> iterator()
    {
        List<String> components = new ArrayList<>();
        if (isRoot())
        {
            return components.iterator();
        }
        Collections.addAll(components, pathString.split(DELIMITER));
        return components.iterator();
    }

    /** Lists the paths of all files in a directory tree on the local
        filesystem.

        @param directory The root directory of the directory tree.
        @return An array of relative paths, one for each file in the directory
                tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                                         does not refer to a directory.
     */
    public static Path[] list(File directory) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    /** Determines whether the path represents the root directory.

        @return <code>true</code> if the path does represent the root directory,
                and <code>false</code> if it does not.
     */
    public boolean isRoot()
    {
        int numElements = stringToPath(this.pathString).getNameCount();
        return (numElements == 0);
    }

    /** Returns the path to the parent of this path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no parent.
     */
    public Path parent()
    {
        java.nio.file.Path parent = stringToPath(this.pathString).getParent();
        if (parent == null)
        {
            throw new IllegalArgumentException("Path represents the root directory");
        }
        Path parentPath = new Path(parent.toString());
        return parentPath;
    }

    /** Returns the last component in the path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no last
                                         component.
     */
    public String last()
    {
        java.nio.file.Path fileName = stringToPath(this.pathString).getFileName();
        if (fileName == null)
        {
            throw new IllegalArgumentException("Path represents the root");
        }
        return fileName.toString();
    }

    /** Determines if the given path is a subpath of this path.

        <p>
        The other path is a subpath of this path if it is a prefix of this path.
        Note that by this definition, each path is a subpath of itself.

        @param other The path to be tested.
        @return <code>true</code> If and only if the other path is a subpath of
                this path.
     */
    public boolean isSubpath(Path other)
    {
        int otherLen = stringToPath(other.pathString).getNameCount();
        int pathLen = stringToPath(this.pathString).getNameCount();
        if (otherLen <= pathLen)
        {
            if (otherLen == 0)
            {
                return true;
            }
            java.nio.file.Path prefixPath = stringToPath(this.pathString).subpath(0, otherLen);
            java.nio.file.Path otherPrefixPath = stringToPath(other.pathString).subpath(0, otherLen);

            return prefixPath.equals(otherPrefixPath);

        }
        return false;
    }

    /** Converts the path to <code>File</code> object.

        @param root The resulting <code>File</code> object is created relative
                    to this directory.
        @return The <code>File</code> object.
     */

    // refer. no clue how to do this.
    public File toFile(File root)
    {
        throw new UnsupportedOperationException("not implemented");
    }

    /** Compares this path to another.

        <p>
        An ordering upon <code>Path</code> objects is provided to prevent
        deadlocks between applications that need to lock multiple filesystem
        objects simultaneously. By convention, paths that need to be locked
        simultaneously are locked in increasing order.

        <p>
        Because locking a path requires locking every component along the path,
        the order is not arbitrary. For example, suppose the paths were ordered
        first by length, so that <code>/etc</code> precedes
        <code>/bin/cat</code>, which precedes <code>/etc/dfs/conf.txt</code>.

        <p>
        Now, suppose two users are running two applications, such as two
        instances of <code>cp</code>. One needs to work with <code>/etc</code>
        and <code>/bin/cat</code>, and the other with <code>/bin/cat</code> and
        <code>/etc/dfs/conf.txt</code>.

        <p>
        Then, if both applications follow the convention and lock paths in
        increasing order, the following situation can occur: the first
        application locks <code>/etc</code>. The second application locks
        <code>/bin/cat</code>. The first application tries to lock
        <code>/bin/cat</code> also, but gets blocked because the second
        application holds the lock. Now, the second application tries to lock
        <code>/etc/dfs/conf.txt</code>, and also gets blocked, because it would
        need to acquire the lock for <code>/etc</code> to do so. The two
        applications are now deadlocked.

        @param other The other path.
        @return Zero if the two paths are equal, a negative number if this path
                precedes the other path, or a positive number if this path
                follows the other path.
     */
    @Override
    public int compareTo(Path other)
    {
        throw new UnsupportedOperationException("not implemented");
    }

    /** Compares two paths for equality.

        <p>
        Two paths are equal if they share all the same components.

        @param other The other path.
        @return <code>true</code> if and only if the two paths are equal.
     */
    @Override
    public boolean equals(Object other)
    {
        throw new UnsupportedOperationException("not implemented");
    }

    /** Returns the hash code of the path. */
    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException("not implemented");
    }

    /** Converts the path to a string.

        <p>
        The string may later be used as an argument to the
        <code>Path(String)</code> constructor.

        @return The string representation of the path.
     */
    @Override
    public String toString()
    {
        throw new UnsupportedOperationException("not implemented");
    }
}
