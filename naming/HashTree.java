package naming;

import common.Path;
import rmi.RMIException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Created by mohit on 12/4/17.
 */

enum OperationMode{
    CREATEDIR, CREATEFILE, DELETE, ISDIR
}

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
                    // remove replicates.
                    removeWriteReplicas (lastComp, path);
                }
            }
            else{
                // read block
                lastComp.lockRead();
                if (lastComp.hashtable == null)
                {
                    // replicate and add all to hashtable
                    addReadReplicas (lastComp, path);
                }
            }
        }
    }

    // referred.
    public boolean removeWriteReplicas(HashNode parentNode, Path fileName)
    {
        while (parentNode.serverStubList.size() > 1)
        {
            ServerStub removedStub = parentNode.serverStubList.removeLast();

            try {
                removedStub.commandStub.delete(fileName);
            } catch (RMIException e) {
                e.printStackTrace();
            }

        }
        return true;
    }

    public boolean addReadReplicas (HashNode parentNode, Path fileName)
    {
        synchronized (parentNode)
        {
            for (ServerStub serverStub : serverStubList){
                if (parentNode.serverStubList.contains(serverStub))
                    continue;

                try {
                    serverStub.commandStub.copy(fileName, parentNode.serverStubList.get(0).storageStub);
                } catch (RMIException e) {
                    e.printStackTrace();
                    continue;
                } catch (IOException e) {
                    e.printStackTrace();
                    continue;
                }

                // add this server to the server list and return;
                parentNode.serverStubList.add(serverStub);
                return true;
            }
        }
        return false;
    }


    public void unlock (Path path, boolean exclusiveLock)
    {
        if (path.isRoot())
        {
            if (exclusiveLock)
            {
                root.unlockWrite();
            }
            else
            {
                root.unlockRead();
            }
            return;
        }
        Iterator<String> iterator = path.iterator();
        unlockRecursive (root, iterator, exclusiveLock);
    }

    private void unlockRecursive (HashNode root, Iterator<String> iterator, boolean exclusiveLock)
    {
        String nextDir = iterator.next();
        if (iterator.hasNext())
        {
            if(root.hasDirectory(nextDir))
            {
                unlockRecursive(root, iterator, exclusiveLock);
            }
            else
            {
                throw new IllegalArgumentException("Name doest exist in subroot or name is a file in root");
            }
        }
        else
        {
            HashNode lastComp = root.getChild(nextDir);
            if (exclusiveLock)
            {
                lastComp.unlockWrite();
            }
            else
            {
                lastComp.unlockRead();
            }
        }
        root.unlockRead();

    }

    public boolean isDirectory (Path path) throws FileNotFoundException {
        lock(path.parent(), false);
        Iterator<String> iterator = path.iterator();
        boolean flag = nameServerOperator(OperationMode.ISDIR, root, iterator, null);
        unlock(path.parent(), false);
        return flag;
    }

    private boolean nameServerOperator (OperationMode mode, HashNode root, Iterator<String> iterator, ServerStub serverStub) throws FileNotFoundException {
        return nameServerOperator(mode, root, iterator, serverStub, null);
    }

    private boolean nameServerOperator (OperationMode mode, HashNode root, Iterator<String> iterator, ServerStub serverStub, Path path) throws FileNotFoundException {
        if(!iterator.hasNext())
        {
            if (mode == OperationMode.CREATEDIR || mode == OperationMode.CREATEFILE || mode == OperationMode.DELETE)
            {
                return false;
            }

            if (mode == OperationMode.ISDIR)
                return true;
        }

        String nextDir = iterator.next();
        if (iterator.hasNext())
        {
            return nameServerOperator(mode, root.getChild(nextDir), iterator, serverStub, null);
        }
        else
        {
            if (mode == OperationMode.ISDIR)
            {
                if (root.hasDirectory(nextDir))
                    return true;
                if (root.hasFile(nextDir))
                    return false;

                throw new FileNotFoundException("File not found in the directory in any storage server");
            }
            else
            {
                if (root.hasDirectory(nextDir) || root.hasFile(nextDir))
                {
                    if (mode == OperationMode.DELETE)
                    {
                        root.delete (nextDir, path);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    if (mode == OperationMode.DELETE)
                    {
                        return false;
                    }
                    else if (mode == OperationMode.CREATEDIR || mode == OperationMode.CREATEFILE)
                    {
                        root.create (nextDir, serverStub);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
        }
        // i guess wont reach here.
    }

    private class HashNode {
        private LinkedList<ServerStub> serverStubList = null;
        private Hashtable<String, HashNode> hashtable = null;
        ReadWriteLock lock = new ReentrantReadWriteLock();

        public HashNode() {
            hashtable = new Hashtable<>();
        }

        public HashNode (ServerStub serverStub)
        {
            this.serverStubList.add(serverStub);
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

        public void unlockWrite ()
        {
            lock.writeLock().unlock();
        }

        public boolean hasFile (String filename)
        {
            HashNode hashNode = hashtable.get(filename);
            if (hashNode != null && hashNode.serverStubList != null)
                return true;
            return false;
        }

        public void create(String fileName, ServerStub serverStub)
        {
            HashNode node;
            if (serverStub == null)
            {
                node = new HashNode();
            }
            else
            {
                node = new HashNode(serverStub);
            }
            this.hashtable.put(fileName, node);

        }

        public void delete (String filename, Path path)
        {
            HashNode child = this.getChild(filename);
            for (ServerStub serverStub : child.getAllStubs())
            {
                try {
                    serverStub.commandStub.delete(path);
                } catch (RMIException e) {
                    e.printStackTrace();
                }
            }
            this.hashtable.remove(filename);
        }

        public HashSet<ServerStub> getAllStubs ()
        {
            if (this.hashtable == null)
            {
                return new HashSet<>(this.serverStubList);
            }
            HashSet<ServerStub> serverStubs = new HashSet<>();
            for (String path : this.hashtable.keySet())
            {
                serverStubs.addAll(this.hashtable.get(path).getAllStubs());
            }
            return serverStubs;
        }


    }
}


