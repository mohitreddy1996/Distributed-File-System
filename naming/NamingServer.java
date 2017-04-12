package naming;

import common.Path;
import rmi.RMIException;
import rmi.Skeleton;
import storage.Command;
import storage.Storage;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

class ServerStub {
    public Storage storageStub;
    public Command commandStub;

    public ServerStub (Storage storageStub, Command commandStub) {
        this.storageStub = storageStub;
        this.commandStub = commandStub;
    }

    public boolean equals (ServerStub compareStub) {
        return storageStub.equals(compareStub.storageStub) && commandStub.equals(compareStub.commandStub);
    }
}

/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration
{
    private boolean started = false;
    private boolean stopped = false;
    private Skeleton<Registration> registrationSkeleton;
    private Skeleton<Service> serviceSkeleton;
    private LinkedList<ServerStub> serverStubList = new LinkedList<>();
    private HashTree hashTree = new HashTree(serverStubList);
    /** Creates the naming server object.

        <p>
        The naming server is not started.
     */
    public NamingServer()
    {
    }

    /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {
        if (started){
            throw new RMIException("Naming Server already started");
        }

        if (stopped){
            throw new RMIException("Naming Server already stopped");
        }

        registrationSkeleton = new Skeleton<Registration>(Registration.class, this, new InetSocketAddress(NamingStubs.REGISTRATION_PORT));

        serviceSkeleton = new Skeleton<Service>(Service.class, this, new InetSocketAddress(NamingStubs.SERVICE_PORT));
        // start the registration and service skeletons.
        registrationSkeleton.start();
        serviceSkeleton.start();

        started = true;

    }

    /** Stops the naming server.

        <p>
        This method commands both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
        // similar to storage server.
        synchronized (this){
            stopped = true;
        }

        registrationSkeleton.stop();
        serviceSkeleton.stop();
        synchronized (this){
            stopped = false;
            started = false;
        }
        stopped(null);


    }

    /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following public methods are documented in Service.java.
    @Override
    public void lock(Path path, boolean exclusive) throws FileNotFoundException
    {
        hashTree.lock(path, exclusive);
    }

    @Override
    public void unlock(Path path, boolean exclusive)
    {
        hashTree.unlock(path, exclusive);
    }

    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException
    {
        return hashTree.isDirectory(path);
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean createFile(Path file)
        throws RMIException, FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean delete(Path path) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {
        throw new UnsupportedOperationException("not implemented");
    }
}
