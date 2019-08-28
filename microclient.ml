open Zmq_lwt
open Core

open Lib
open Log

let start_zmq_socket op_path = 
  Lwt.return (
  let ctx = Zmq.Context.create() in
  let socket = Zmq.Socket.create ctx Zmq.Socket.req in
  let () = Zmq.Socket.connect socket ("ipc://"^op_path) in
  Socket.of_socket socket
  )

let recv_op socket =
  let%lwt payload = Socket.recv socket in
  Lwt.return (Message_pb.decode_operation (Pbrt.Decoder.of_bytes (Bytes.of_string payload)))

let conv_exn f x = match f x with
  | Some(v) -> v
  | None -> assert false

let do_put ({key; value;_}:Message_types.operation_op_put) client =
  let cmd = Types.Create (conv_exn Int64.to_int key, Bytes.to_string value) in
  let st = Unix.gettimeofday() in 
  let%lwt () = Client.send_request_message client cmd in
  let et = Unix.gettimeofday() in
  Lwt.return (st, et, "Write", "")
  
let do_get ({key;_}:Message_types.operation_op_get) client = 
  let cmd = Types.Read (conv_exn Int64.to_int key) in
  let st = Unix.gettimeofday() in 
  let%lwt () = Client.send_request_message client cmd in
  let et = Unix.gettimeofday() in
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

let client_port = 2382

let run_client host uris op_path client_id =
  let log_directory = "client-" ^ host ^ "-" ^ (string_of_int client_port) in
  let client_id = conv_exn Int32.of_int client_id in
  Lwt_main.run begin
    let%lwt () = Logger.initialize_default log_directory in
    let%lwt () = Lwt_io.printl "Client: Spinning up" in
    let%lwt client = Client.new_client host client_port uris in
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
            endpoints   = anon ("endpoints" %: string)
        and my_ip       = anon ("my_ip"     %: string)
        and client_id   = anon ("client_id" %: int)
        and op_path     = anon ("op_path"   %: string)
      in fun () ->
        let replica_uris = List.map (assert false) 
          ~f:(fun host -> Lib.Message.uri_from_address host 2381)
        in
        run_client
          my_ip 
          replica_uris
          op_path
          client_id
    )

let () = Command.run command
