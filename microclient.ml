open Zmq_lwt
open Core

open Lib

let start_zmq_socket op_path = 
  Lwt.return (
  let ctx = Zmq.Context.create() in
  let socket = Zmq.Socket.create ctx Zmq.Socket.req in
  let () = Zmq.Socket.connect socket ("ipc://"^op_path) in
  Socket.of_socket socket
  )

let recv_op socket = 
  let%lwt () = Lwt_io.printl "Client: Waiting to receive op" in
  let%lwt payload = Socket.recv socket in
  let%lwt () = Lwt_io.printl "Client: Received op" in
  Lwt.return (Message_pb.decode_operation (Pbrt.Decoder.of_bytes (Bytes.of_string payload)))

let conv_exn f x = match f x with
  | Some(v) -> v
  | None -> assert false

let do_put ({key; value;_}:Message_types.operation_op_put) client =
  let key = Int64.to_string key in
  let value = Bytes.to_string value in
  let st = Unix.gettimeofday() in 
  let%lwt _ = Client.C_Lwt.op_create client key value in
  let et = Unix.gettimeofday() in
  let%lwt () = Lwt_io.printl "Client: Put sucessful" in
  Lwt.return (st, et, "Write", "")
  
let do_get ({key;_}:Message_types.operation_op_get) client = 
  let key = Int64.to_string key in
  let st = Unix.gettimeofday() in 
  let%lwt _ = Client.C_Lwt.op_read client key in
  let et = Unix.gettimeofday() in
  let%lwt () = Lwt_io.printl "Client: Get sucessful" in
  Lwt.return (st, et, "Read", "")

let rec main_loop socket client encoder  clientid =
  let%lwt op = recv_op socket in
  let%lwt s_time_stamps_x_resp = 
    match op with
    | Put(op) -> Lwt.return @@ Some(op.start, do_put op client)
    | Get(op) -> Lwt.return @@ Some(op.start, do_get op client)
    | Quit(op) -> 
        let%lwt () = Lwt_io.printl ("Quitting msg: " ^ op.msg) in
        Lwt.return None
  in
  match s_time_stamps_x_resp with
  | Some(queue_start, v) -> 
      let%lwt (st, et, op_type, err) = v in
      let resp = Message_types.({
        err = err;
        client_start = st;
        queue_start = queue_start;
        end_ = et;
        clientid = clientid;
        optype = op_type;
        response_time = -1.;
        target = "";
      }) in
      let () = Message_pb.encode_response resp encoder in
      let payload = Bytes.to_string @@ Pbrt.Encoder.to_bytes encoder in
      let%lwt () = Socket.send socket payload in
      main_loop socket client encoder clientid
  | None -> Lwt.return_unit

let run_client uris op_path client_id =
  let client_id = conv_exn Int32.of_int client_id in
  Lwt_main.run begin
    let%lwt () = Lwt_io.printl "Spinning up client" in
    let client = Client.new_client uris in
    let%lwt socket = start_zmq_socket op_path in
    let%lwt () = Socket.send socket "" in
    let encoder = Pbrt.Encoder.create () in
    main_loop socket client encoder client_id
  end

let command = 
  Command.basic
    ~summary:"microclient for OCaml Paxos"
    Command.Let_syntax.(
      let%map_open
          endpoints = anon ("endpoints" %: string)
      and op_path = anon ("op_path" %: string)
      and client_id = anon ("id" %: int)
      in fun () ->
        run_client endpoints op_path client_id
    )

let () = Command.run command
