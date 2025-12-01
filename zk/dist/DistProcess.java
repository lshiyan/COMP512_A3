/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva; ©2024, Bettina Kemme; ©2025, Olivier Michaud
*/
import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;

// TODO
// Replace 26 with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your manager process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your manager's logic and worker's logic.
//		This is important as both the manager and worker may need same kind of callbacks and could result
//			with the same callback functions.
// REMEMBER !! Managers and Workers are also clients of ZK and the ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
// In particular, if the process is a worker, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.


public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback, AsyncCallback.DataCallback, AsyncCallback.StatCallback
{
    ZooKeeper zk;
    String zkServer, pinfo;
    boolean isManager=false;
    boolean initialized=false;
    String managerPath = "/dist26/manager";
    String workersPath = "/dist26/workers";
    String tasksPath = "/dist26/tasks";
    String workerPath;
    Set<String> aliveWorkers = new HashSet<>();
    Set<String> tasksWithResults = new HashSet<>();  // Track completed tasks
    final long TIME_SLICE_MS = 500; //Timeout for tasks.

    DistProcess(String zkhost)
    {
        zkServer=zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
    {
        zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
    }

    void initialize()
    {
        try
        {
            runForManager();	
            isManager=true;
            getTasks(); 
            getWorkers();

        } catch(NodeExistsException nee)
        {
            isManager=false;
            try {
                registerAsWorker();
            } catch(KeeperException | InterruptedException e) {
                System.out.println("Error registering worker: " + e);
            }
        }
        catch(UnknownHostException uhe)
        { System.out.println(uhe); }
        catch(KeeperException ke)
        { System.out.println(ke); }
        catch(InterruptedException ie)
        { System.out.println(ie); }

        System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isManager?"manager":"worker"));

    }
    // manager fetching worker znodes
    void getWorkers() {zk.getChildren(workersPath, this, this, null);}

    // Manager fetching task znodes...
    void getTasks() {zk.getChildren(tasksPath, this, this, null);}

    // tries to create workers path (if it exsits ignore), then creates the path to specific worker
    void registerAsWorker() throws KeeperException, InterruptedException
    {
        try
        {
            zk.create(workersPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        catch(NodeExistsException ignore){}

        workerPath = workersPath + "/" + pinfo.replace('@','-');
        zk.create(workerPath, "idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("DISTAPP : Worker registered and watching for assignments: " + workerPath);
        zk.getData(workerPath, this, this, null);
    }

    // Try to become the manager.
    void runForManager() throws UnknownHostException, KeeperException, InterruptedException
    {
        //Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
        // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        zk.create(managerPath, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void process(WatchedEvent e)
    {
        //Get watcher notifications.

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library 
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        System.out.println("DISTAPP : Event received : " + e);

        // not mannager and watcher activated from worker path (so an assignment)
        if (!isManager && workerPath != null && e.getPath() != null && e.getPath().equals(workerPath)
            && e.getType() == Watcher.Event.EventType.NodeDataChanged) {
            System.out.println("DISTAPP : Worker detected assignment change");
            zk.getData(workerPath, this, this, null);  
        }
        
        // a change in the workers path (so a worker has connected or disconnected)
        if (isManager && e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals(workersPath)) {
            getWorkers();
        }

        if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
        {
            // Once we are connected, do our intialization stuff.
            if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initialized == false) 
            {
                initialize();
                initialized = true;
            }
        }

        // Manager should be notified if any new znodes are added to tasks.
        if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && tasksPath.equals(e.getPath()))
        {
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the children.
            getTasks();
        }
    }

    //Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children)
    {
        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library 
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        // This logic is for manager !!
        //Every time a new task znode is created by the client, this will be invoked.

        System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);

        //tryAssignTasks callback to find one unassigned task
        if (ctx != null && ctx.equals("assign")) {
            assignOneTask(children);
            return;
        }

        // for manager, update worker list and trigger assignment
        if (isManager && path.equals(workersPath)) {
            System.out.println("DISTAPP : Manager updated worker list: " + children);
            aliveWorkers.clear();
            aliveWorkers.addAll(children);
            tryAssignTasks();
            return;
        }

        // for manager, to handle task list changes
        if (isManager && path.equals(tasksPath)) {
            System.out.println("DISTAPP : Manager received task list: " + children);
            for (String taskId : children) {
                zk.exists(tasksPath + "/" + taskId + "/result", false, this, taskId);
            }
            tryAssignTasks();
        }
    }

    // method to assign pending tasks to idle workers (wait for zk children callback)
    void tryAssignTasks() {
        if (!isManager) return;

        System.out.println("DISTAPP : Manager trying to assign tasks");
        zk.getChildren(tasksPath, false, this, "assign");
    }

    // helper, find not assigned taskss and assign it
    void assignOneTask(List<String> tasks) {
        for (String taskId : tasks) {
            if (!tasksWithResults.contains(taskId)) {
                findIdleWorkerAndAssign(taskId);
                break; 
            }
        }
    }

    // for manager, find an idle worker and assign the task (wait for zk data callback)
    void findIdleWorkerAndAssign(String taskId) {
        if (aliveWorkers.isEmpty()) {
            System.out.println("DISTAPP : Manager: No workers available for task " + taskId);
            return;
        }

        String workerId = aliveWorkers.iterator().next();
        String workerNodePath = workersPath + "/" + workerId;
        zk.getData(workerNodePath, false, this, taskId + ":" + workerId);
    }

    //after receiving the data callback, you assign by changing idle to the task number
    void checkWorkerAndAssign(String path, byte[] data, String taskId, String workerId) {
        if (data != null) {
            String status = new String(data);
            if (status.equals("idle")) {
                System.out.println("DISTAPP : Manager assigning task " + taskId + " to worker " + workerId);
                zk.setData(path, taskId.getBytes(), -1, this, "assigned:" + taskId);
            }
        }
    }

    // callback for checking if result exsits (task completion) or assignment success based of ctx (callback from zk.setData)
    public void processResult(int rc, String path, Object ctx, org.apache.zookeeper.data.Stat stat) {
        if (ctx == null) return;

        String ctxStr = (String) ctx;
        if (ctxStr.startsWith("assigned:")) {
            if (rc == 0) {
                String taskId = ctxStr.substring(9);
                System.out.println("DISTAPP : Manager successfully assigned task " + taskId);
            }
            return;
        }

        // Handle result check
        if (rc == 0 && stat != null) {
            tasksWithResults.add(ctxStr);
            System.out.println("DISTAPP : Task " + ctxStr + " has result");
        }
    }

    // data call back from zk.getData, tries to assign tasks to workers
    public void processResult(int rc, String path, Object ctx, byte[] data, org.apache.zookeeper.data.Stat stat) {
        if (rc != 0) return;

        if (!isManager && path.equals(workerPath) && data != null) {
            String assignment = new String(data);
            System.out.println("DISTAPP : Worker received assignment: " + assignment);

            if (!assignment.equals("idle")) {
                final String taskId = assignment;
                //exectue task in a new thread
                new Thread(() -> {
                    executeTask(taskId);
                }).start();
            }
            return;
        }

        // Manager: Handle checking worker status for assignment
        if (isManager && ctx != null) {
            String ctxStr = (String) ctx;
            if (ctxStr.contains(":")) {
                String[] parts = ctxStr.split(":");
                String taskId = parts[0];
                String workerId = parts[1];
                checkWorkerAndAssign(path, data, taskId, workerId);
            }
        }
    }

    // execute the assigned task in a separate thread (worker only)
    void executeTask(String taskId) {
        System.out.println("DISTAPP : Worker executing task: " + taskId);
        try {
            DistTask dt;

            // Retrieve task (either from memory or ZK)
            byte[] taskSerial = zk.getData(tasksPath + "/" + taskId, false, null);
            ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
            ObjectInput in = new ObjectInputStream(bis);
            dt = (DistTask) in.readObject();

            // dt is now effectively final
            final DistTask task = dt;
            AtomicBoolean interruptedFlag = new AtomicBoolean(false);

            Thread computeThread = new Thread(() -> {
                try {
                    task.compute();
                } catch (InterruptedException e) {
                    interruptedFlag.set(true);
                    System.out.println("DISTAPP : Task interrupted, partial state saved in-memory");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            computeThread.start();

            // Time slicing logic
            computeThread.join(TIME_SLICE_MS);

            if (computeThread.isAlive()) {
                System.out.println("DISTAPP : Interrupting task (time slice expired)");
                interruptedFlag.set(true);
                computeThread.interrupt();
            }

            computeThread.join(); // Wait for thread termination

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(task);
            oos.flush();
            byte[] outputBytes = bos.toByteArray();
            // Decide full completion vs partial
            if (!interruptedFlag.get()) {

                zk.create(tasksPath + "/" + taskId + "/result",
                        outputBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                System.out.println("DISTAPP : Worker completed task: " + taskId);
            } else {
                
                zk.setData(tasksPath + "/" + taskId, outputBytes, -1);
                System.out.println("DISTAPP : Task partially executed, saved in worker memory");
            }

            // Mark worker idle
            zk.setData(workerPath, "idle".getBytes(), -1);
            System.out.println("DISTAPP : Worker back to idle");

        } catch (Exception e) {
            e.printStackTrace();
            try { zk.setData(workerPath, "idle".getBytes(), -1); } catch (Exception ignore) {}
        }
    }


    public static void main(String args[]) throws Exception
    {
        //Create a new process
        //Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();

        // Keep the process running forever (until interrupted)
        synchronized(dt) {
            dt.wait();
        }
    }
}
