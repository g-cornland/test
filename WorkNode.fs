open System
open System.IO
open System.IO.Compression
open System.IO.MemoryMappedFiles
open System.Collections.Generic
open System.Collections.Concurrent
open System.Text
open System.Security.Cryptography

open NetMQ
open NetMQ.Sockets

let CHUNKSIZE = 1024 * 1024
let ENTRYNAME = "Node_Entry"
let ENTRYADDR = "tcp://127.0.0.1:5555"
let SELFNAME = "Node_1"
let SELFADDR = "tcp://127.0.0.1:5556"
let FILE = @"D:\Software\Photoshop-2023-v24.1.0.166-x64-zh_CN-Portable.7z"
let RCVDIR = @"E:\Tmp\receive\"
let mutable reqid = ref 0L
let mutable starttime = DateTime.UtcNow

type TransferTask = 
    struct
       val mutable FileName : string
       val mutable FileSize : int64
       val mutable ChunkSize: int
       val mutable ChunkHash: Dictionary<int, string>
       val mutable mmFile: MemoryMappedFile
       new(fname, fsize, chksize) = { FileName = fname; FileSize = fsize; ChunkSize = chksize; ChunkHash = new Dictionary<int, string>(); mmFile = MemoryMappedFile.CreateFromFile(RCVDIR + fname, FileMode.Create, null, fsize) }
    end

let saveTransferTask(file:string, tt:TransferTask) =
    let fs = new FileStream(file, FileMode.Create)
    let sw = new StreamWriter(fs)
    // file summary
    sw.WriteLine(tt.FileName)
    sw.WriteLine(tt.FileSize)
    sw.WriteLine(tt.ChunkSize)
    // chunk item
    for kv in tt.ChunkHash do
        sw.WriteLine(kv.Key)
        sw.WriteLine(kv.Value)
    sw.Flush();
    sw.Close();
    fs.Close();

let readTransferTask(file:string) : TransferTask =
    let fs = new FileStream(file, FileMode.Open)
    let sr = new StreamReader(fs)
    let mutable tt = new TransferTask()
    tt.FileName <- sr.ReadLine()
    tt.FileSize <- Convert.ToInt32(sr.ReadLine())
    tt.ChunkSize <- Convert.ToInt32(sr.ReadLine())
    let mutable nChunk = int(tt.FileSize / int64(tt.ChunkSize))
    if int64(nChunk * tt.ChunkSize) < tt.FileSize then
        nChunk <- nChunk + 1
    let mutable i = 0
    while (i < nChunk) && (not sr.EndOfStream) do
        tt.ChunkHash.Add(Convert.ToInt32(sr.ReadLine()), sr.ReadLine())
        i <- i + 2
    sr.Close()
    fs.Close()
    tt

let mergeChunk(rcvdir:string, tt:TransferTask) = 
    let mutable chunkNum = int(tt.FileSize / int64(tt.ChunkSize))
    if int64(chunkNum * tt.ChunkSize) < tt.FileSize then
        chunkNum <- chunkNum + 1
    assert (chunkNum = tt.ChunkHash.Count)
    let md5 = System.Security.Cryptography.MD5.Create()
    for kv in tt.ChunkHash do
        //let chunkFile = rcvdir + tt.FileName + "." + kv.Key.ToString()
        //let fs = new FileStream(chunkFile, System.IO.FileMode.Open)
        let offset = (kv.Key - 1) * tt.ChunkSize
        let mutable size = int64(tt.ChunkSize)
        if kv.Key = chunkNum then
            size <- tt.FileSize - int64(chunkNum - 1) * int64(tt.ChunkSize)
        let vs = tt.mmFile.CreateViewStream(offset, size)
        let bytehash = md5.ComputeHash(vs)
        vs.Close()
        let hash = BitConverter.ToString(bytehash).Replace("-", String.Empty)
        assert (hash = kv.Value)
        //fs.Close()
    //let fs = new FileStream(rcvdir + tt.FileName, FileMode.Create)
    //let bw = new BinaryWriter(fs)
    //for i = 1 to chunkNum do
        //bw.Write(File.ReadAllBytes(rcvdir + tt.FileName + "." + i.ToString()))
    //bw.Flush()
    //bw.Close()
    //fs.Close()

let sendfile(file:string, targetid:string, outSocket:RouterSocket) = 
    let msg = new NetMQMessage()
    // FILE.TRANSFER.FILEINFO
    msg.Append(targetid)
    let sn = System.Threading.Interlocked.Increment reqid
    msg.Append(sn)
    msg.Append("FILE.TRANSFER.FILEINFO")
    let fi = FileInfo(file)
    msg.Append(fi.Name)
    msg.Append("FILE.TRANSFER.FILESIZE")
    msg.Append(fi.Length)
    msg.Append("FILE.TRANSFER.CHUNKSIZE")
    msg.Append(CHUNKSIZE)
    outSocket.SendMultipartMessage(msg)

    // FILE.TRANSFER.CHUNKPUSH
    let fs = new FileStream(file, FileMode.Open)
    let br = new BinaryReader(fs)
    let md5 = System.Security.Cryptography.MD5.Create()
    let mutable i = 0
    while br.BaseStream.Position < br.BaseStream.Length do
        let buf = br.ReadBytes(CHUNKSIZE)
        let bytehash = md5.ComputeHash(buf)
        let hash = BitConverter.ToString(bytehash).Replace("-", String.Empty)
        msg.Clear()
        msg.Append(targetid)
        let sn = System.Threading.Interlocked.Increment reqid
        msg.Append(sn)
        msg.Append("FILE.TRANSFER.CHUNKPUSH")
        msg.Append(fi.Name)
        msg.Append("FILE.TRANSFER.CHUNKIDX")
        i <- i + 1
        msg.Append(i)
        msg.Append("FILE.TRANSFER.CHUNKHASH")
        msg.Append(hash)
        msg.Append("FILE.TRANSFER.CHUNKDATA")
        msg.Append(buf)
        outSocket.SendMultipartMessage(msg)
        System.Console.WriteLine("chunk sent: " + i.ToString())

    // FILE.TRANSFER.DONE
    msg.Clear()
    msg.Append(targetid)
    let sn = System.Threading.Interlocked.Increment reqid
    msg.Append(sn)
    msg.Append("FILE.TRANSFER.DONE")
    msg.Append(fi.Name)
    outSocket.SendMultipartMessage msg
    System.Console.WriteLine("done")

[<EntryPoint>]
let main args =

    // cluster network, name, address
    let mutable nodemap = new Dictionary<string, string>()

    // file transfer tasks
    let mutable taskmap = new Dictionary<string, TransferTask>()

    // received reply msg
    let mutable replymap = new ConcurrentDictionary<int64, NetMQMessage>()

    // event for build connection
    let mutable evtConnected = new System.Threading.AutoResetEvent(false)

    let mutable inSocket = new RouterSocket()
    inSocket.Options.Identity <- System.Text.Encoding.ASCII.GetBytes(SELFNAME + "_in")
    inSocket.Bind SELFADDR

    let mutable outSocket = new RouterSocket()
    outSocket.Options.Identity <- System.Text.Encoding.ASCII.GetBytes(SELFNAME + "_out")
    
    let send_reg(targetid:string, targetaddr:string, cmdtype:string) =
        outSocket.Connect targetaddr
        let msg = new NetMQMessage()
        msg.Append (targetid)
        msg.Append reqid.Value
        msg.Append cmdtype
        msg.Append SELFADDR
        outSocket.SendMultipartMessage msg

    let send_regged(targetid:string) = 
        let msg = new NetMQMessage()
        msg.Append (targetid)
        msg.Append reqid.Value
        msg.Append "NETWORK.REGGED"
        msg.Append SELFADDR
        outSocket.SendMultipartMessage msg        
    
    (fun e -> 
        // targetid/incomeid, requestid, [cmdtype, data], [cmdtype, data]...
        let frames = inSocket.ReceiveMultipartMessage()
        assert(frames.FrameCount >= 3)
        let incomeid = frames[0].ConvertToString()
        let newname = incomeid.Substring(0, incomeid.LastIndexOf("_"))
        let targetid = newname + "_in"
        let cmdtype = frames[2].ConvertToString()
        System.Console.WriteLine("received: " + cmdtype)
        match cmdtype with
        // build network
        | "NETWORK.REG2NODE" ->
            assert (frames.FrameCount = 4)
            let newNodeAddr = frames[3].ConvertToString()
            if not (nodemap.ContainsValue(newNodeAddr)) then
                send_reg(targetid, newNodeAddr, "NETWORK.REP2NODE")

        | "NETWORK.REP2NODE" ->
            assert (frames.FrameCount = 4)
            if (not ((evtConnected.WaitOne(1)) || nodemap.ContainsKey(newname))) then
                evtConnected.Set() |> ignore
                nodemap.Add(newname, frames[3].ConvertToString())
                send_regged(targetid)

        | "NETWORK.REGGED" ->
            assert (frames.FrameCount = 4)
            nodemap.Add(newname, frames[3].ConvertToString())

        | "NETWORK.GETNODELIST" ->
            assert (frames.FrameCount = 3)
            let msg = new NetMQMessage()
            msg.Append targetid
            msg.Append frames[1]
            msg.Append "NETWORK.NODELIST"
            if nodemap.Count > 0 then
                for kv in nodemap do
                    msg.Append kv.Key
                    msg.Append kv.Value
            outSocket.SendMultipartMessage msg

        | "NETWORK.NODELIST" ->
            assert ((frames.FrameCount - 3) % 2 = 0)
            let requestid = frames[1].ConvertToInt64()
            frames.Pop() |> ignore
            frames.Pop() |> ignore
            frames.Pop() |> ignore
            replymap[requestid] <- frames

        // command file
        | "FILE.TRANSFER.GETFILE" ->
            assert (frames.FrameCount = 3)
            //test sendfile
            sendfile(FILE, targetid, outSocket)

        | "FILE.TRANSFER.FILEINFO" -> 
            starttime <- DateTime.UtcNow
            assert (frames.FrameCount = 8)
            assert (frames[4].ConvertToString() = "FILE.TRANSFER.FILESIZE")
            assert (frames[6].ConvertToString() = "FILE.TRANSFER.CHUNKSIZE")
            let filename = frames[3].ConvertToString()
            let filesize = frames[5].ConvertToInt64()
            let chunksize = frames[7].ConvertToInt32()
            // build TransferTask
            let tt = new TransferTask(filename, filesize, chunksize)
            saveTransferTask(RCVDIR + filename + ".cfg", tt)
            taskmap.Add(filename, tt)

        | "FILE.TRANSFER.CHUNKPUSH" -> 
            assert (frames.FrameCount = 10)
            assert (frames[4].ConvertToString() = "FILE.TRANSFER.CHUNKIDX")
            assert (frames[6].ConvertToString() = "FILE.TRANSFER.CHUNKHASH")
            assert (frames[8].ConvertToString() = "FILE.TRANSFER.CHUNKDATA")
            let filename = frames[3].ConvertToString()
            let iChunk = frames[5].ConvertToInt32()
            let hash = frames[7].ConvertToString()
            let data = frames[9].ToByteArray()
            //let fs = new FileStream(RCVDIR + filename + "." + iChunk.ToString(), FileMode.Create)
            //let bw = new BinaryWriter(fs);
            //bw.Write(data)
            //bw.Flush()
            //bw.Close()
            //fs.Close()
            let vs = taskmap[filename].mmFile.CreateViewStream(int64((iChunk - 1) * (taskmap[filename].ChunkSize)), data.Length)
            let bw = new BinaryWriter(vs)
            bw.Write(data)
            bw.Close()
            vs.Close()
            if taskmap[filename].ChunkHash.ContainsKey(iChunk) then
                taskmap[filename].ChunkHash.Remove(iChunk) |> ignore
            taskmap[filename].ChunkHash.Add(iChunk, hash)
            saveTransferTask(RCVDIR + filename + ".cfg", taskmap[filename])
            System.Console.WriteLine("chunk received: " + iChunk.ToString())

        | "FILE.TRANSFER.DONE" ->
            assert (frames.FrameCount = 4)
            let filename = frames[3].ConvertToString()
            mergeChunk(RCVDIR, taskmap[filename])
            let now = DateTime.UtcNow
            let elapse = now.Subtract(starttime)
            if elapse.TotalSeconds > 0 then
                let speed = float(taskmap[filename].FileSize) / elapse.TotalSeconds
                System.Console.WriteLine("The transfer speed: " + speed.ToString())
            taskmap[filename].mmFile.Dispose()
            System.Console.WriteLine("done")
        //| "FILE.TRANSFER.CHUNKPULL" -> ignore
        //| _ -> ignore
    )
    |> inSocket.ReceiveReady.Add

    let receive_thread() =
        async {
            while true do
                10
                |> System.TimeSpan.FromMilliseconds 
                |> inSocket.Poll 
                |> ignore
        }
        |> Async.Start

    receive_thread()

    // test
    if not (ENTRYNAME = SELFNAME) then
        while not (evtConnected.WaitOne(100)) do
            send_reg(ENTRYNAME + "_in", ENTRYADDR, "NETWORK.REG2NODE")

        // get node list
        let mutable msg = new NetMQMessage()
        msg.Append(ENTRYNAME + "_in")
        let mutable sn = System.Threading.Interlocked.Increment reqid
        msg.Append(sn)
        msg.Append("NETWORK.GETNODELIST")
        outSocket.SendMultipartMessage msg

        // connect to existed nodes
        System.Threading.SpinWait.SpinUntil(fun () -> replymap.ContainsKey(sn))
        msg <- replymap[sn]
        for i = 0 to msg.FrameCount - 1 do
            if msg[i].ConvertToString() <> SELFNAME then
                evtConnected.Reset() |> ignore
                while not (evtConnected.WaitOne(100)) do
                    send_reg(msg[i].ConvertToString() + "_in", msg[i + 1].ConvertToString(), "NETWORK.REG2NODE")

        //==test request a file
        msg.Clear()
        msg.Append(ENTRYNAME + "_in")
        sn <- System.Threading.Interlocked.Increment reqid
        msg.Append(sn)
        msg.Append("FILE.TRANSFER.GETFILE")
        outSocket.SendMultipartMessage msg
        
    let s = System.Console.ReadLine()
    0