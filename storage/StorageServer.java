package storage;

import common.Path;
import naming.Registration;
import rmi.RMIException;
import rmi.Skeleton;
import rmi.Stub;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */
public class StorageServer implements Storage, Command
{
    private File root;
    private int client_port;
    private int command_port;
    private Skeleton<Command> commandSkeleton = null;
    private Skeleton<Storage> storageSkeleton = null;
    // flag to check for only one execution.
    private boolean started = false;
    private boolean stopped = false;
    /** Creates a storage server, given a directory on the local filesystem, and
        ports to use for the client and command interfaces.

        <p>
        The ports may have to be specified if the storage server is running
        behind a firewall, and specific ports are open.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @param client_port Port to use for the client interface, or zero if the
                           system should decide the port.
        @param command_port Port to use for the command interface, or zero if
                            the system should decide the port.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
    public StorageServer(File root, int client_port, int command_port)
    {
        if (root == null){
            throw new NullPointerException("Root can not be null");
        }
        this.root = root;
        this.client_port = client_port;
        this.command_port = command_port;
    }

    /** Creats a storage server, given a directory on the local filesystem.

        <p>
        This constructor is equivalent to
        <code>StorageServer(root, 0, 0)</code>. The system picks the ports on
        which the interfaces are made available.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root)
    {
        this(root, 0, 0);
    }

    /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
        throws RMIException, UnknownHostException, FileNotFoundException
    {
        // create command and client interface.
        if (started){
            throw new RMIException("Storage Server is already started");
        }
        if (stopped){
            throw new RMIException("Storage Server was stopped already.");
        }

        // create the skeletons.
        if (this.client_port == 0){
            storageSkeleton = new Skeleton<Storage>(Storage.class, this);
        }else{
            storageSkeleton = new Skeleton<Storage>(Storage.class, this, new InetSocketAddress(client_port));
        }

        if (this.command_port == 0){
            commandSkeleton = new Skeleton<Command>(Command.class, this);
        }else{
            commandSkeleton = new Skeleton<Command>(Command.class, this, new InetSocketAddress(command_port));
        }

        // start them and create stubs.
        storageSkeleton.start();
        commandSkeleton.start();

        Storage storageStub = Stub.create(Storage.class, storageSkeleton, hostname);
        Command commandStub = Stub.create(Command.class, commandSkeleton, hostname);

        // ToDo: How to get the paths.

        // Register to the name server.
        Path[] duplicates = naming_server.register(storageStub, commandStub, new Path[] {});

    }

    /** Stops the storage server.

        <p>
        The server should not be restarted.
     */
    public void stop()
    {
        synchronized (this){
            stopped = true;
        }

        storageSkeleton.stop();
        commandSkeleton.stop();
        synchronized (this){
            started = false;
            stopped = false;
        }
        stopped(null);
    }

    /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
        File requiredFile = file.toFile(root);
        if (requiredFile.isDirectory() || requiredFile.exists() == false){
            throw new FileNotFoundException("File is either a directory or File not found");
        }
        // size not function like that.
        return requiredFile.length();
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException
    {
        File requiredFile = file.toFile(root);

        // check if it exists.
        if (requiredFile.isDirectory() || requiredFile.exists() == false){
            throw new FileNotFoundException("Input either a directory or File not found");
        }

        // check if it can be read.
        if (requiredFile.canRead()){
            throw new IOException("File not readable.");
        }

        if (offset < 0 || offset + length > requiredFile.length() || length < 0){
            throw new IndexOutOfBoundsException("Offset Negative or Lenght given is negative or Length + offset exceeds file length");
        }

        // Use Random access File Reader.
        RandomAccessFile randomAccessFile = new RandomAccessFile(requiredFile, "r");

        byte[] data = new byte[length];
        randomAccessFile.seek(offset);
        // offset = 0. seek is already done before.
        randomAccessFile.readFully(data, 0, length);

        return data;
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data)
        throws FileNotFoundException, IOException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file)
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public synchronized boolean delete(Path path)
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public synchronized boolean copy(Path file, Storage server)
        throws RMIException, FileNotFoundException, IOException
    {
        throw new UnsupportedOperationException("not implemented");
    }
}
