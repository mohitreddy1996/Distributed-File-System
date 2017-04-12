package naming;

import common.Path;

import java.io.FileNotFoundException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by mohit on 12/4/17.
 */
public class HashTree {
    private HashNode root;
    private LinkedList<ServerStub> serverStubList;

    public HashTree (){

    }

    public HashTree (LinkedList<ServerStub> serverStubList){
        this.serverStubList = serverStubList;
        this.root = new HashNode();
    }

    public void lock (Path path, boolean exclusiveLock) throws FileNotFoundException {
        if (path.isRoot())
        {
            if (exclusiveLock)
            {
                root.lockWrite();
            }
            else
            {
                root.lockRead();
            }
            return;
        }
        Iterator<String> iterator = path.iterator();
        apply_lock_recursive(root, iterator, path, exclusiveLock);
    }

    private void apply_lock_recursive(HashNode root, Iterator<String> iterator, Path path, boolean exclusiveLock) throws FileNotFoundException {
        // we have all the components in the path.
        // keep applying locks on head parent directory and traverse down handling cases.
        String nextDir = iterator.next();

        if (iterator.hasNext())
        {
            root.lockRead();

            if (root.hasDirectory(nextDir))
            {
                apply_lock_recursive(root.getChild(nextDir), iterator, path, exclusiveLock);
            }
            else
            {
                root.unlockRead ();
                throw new FileNotFoundException("The next directory does not exist in root");
            }
        }
        else
        {
            // last component of the path/
            // could be a file or an empty directory.
            root.lockRead();

            HashNode lastComp = root.getChild(nextDir);
            if (exclusiveLock)
            {
                // write.
                lastComp.lockWrite();;
                if (lastComp.hashtable == null)
                {
                    //
                }
            }
            else{
                // read block
                lastComp.lockRead();
                if (lastComp.hashtable == null)
                {
                    //
                }
            }
        }
    }

    private class HashNode {
        private LinkedList<ServerStub> serverStubList = null;
        private Hashtable<String, HashNode> hashtable = null;
        ReadWriteLock lock = new ReentrantReadWriteLock();

        public HashNode() {
            hashtable = new Hashtable<>();
        }

        public void lockWrite ()
        {
            lock.writeLock().lock();
        }

        public void lockRead ()
        {
            lock.readLock().lock();
        }

        public boolean hasDirectory (String rootDir)
        {
            HashNode node = hashtable.get(rootDir);
            if (node != null && node.hashtable != null)
                return true;
            return false;
        }

        public HashNode getChild (String dir)
        {
            return hashtable.get(dir);
        }

        public void unlockRead ()
        {
            lock.readLock().unlock();
        }

    }
}


