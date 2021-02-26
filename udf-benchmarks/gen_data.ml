type 'a range = { low: 'a; high: 'a }

let output_endline chan str =
  output_string chan str;
  output_char chan '\n'

let random_range rand {low; high} =
  rand (high - low) + low

let data_file = ref ""
let num_users = ref 0
let ev_range_low = ref 0
let ev_range_high = ref 0
let boundary_prob = ref 0.0
let num_lookups = ref 0
let lookup_file = ref ""

let weighted_random (type a) (weights: (float * a) array) =
  let r = Random.float 1.0 in
  let exception Found of a in
  try ignore @@ Array.fold_left (fun acc (i, a) ->
      let acc = acc +. i in
      if r < acc
      then raise (Found a)
      else acc
    ) 0.0 weights;
    raise Not_found
  with
  | Found a -> a

let gen () =
  let open Printf in
  assert (not (!boundary_prob >= 0.5));
  let weights = [|(!boundary_prob,1);(!boundary_prob,2);(1.0 -. 2.0 *. !boundary_prob,0)|] in
  let event_range = {low= !ev_range_low; high= !ev_range_high} in
  let chan = open_out !data_file in
  output_endline chan "#clicks";
  output_endline chan "i32,i32,i32";
  for i = 0 to !num_users do
    fprintf chan "%i,2,0\n" i;
    let num_events = random_range Random.int event_range in
    for e = 1 to num_events do
      let ty = weighted_random weights in
      fprintf chan "%i,%i,%i\n" i ty e
    done
  done;
  close_out chan;
  let chan = open_out !lookup_file in
  output_endline chan "#clickstream_ana";
  output_endline chan "i32";
  for _ = 0 to !num_lookups do
    let r = Random.int (!num_users - 1) in
    fprintf chan "%i\n" r
  done;
  close_out chan

let () =
  Arg.(parse [
      "--num-lookups", Set_int num_lookups, "none";
      "--num-users", Set_int num_users, "none";
      "--lookup-file", Set_string lookup_file , "none";
      "--data-file", Set_string data_file, "none";
      "--boundary-prob", Set_float boundary_prob, "none";
      "--ev-range-low", Set_int ev_range_low, "none";
      "--ev-range-high", Set_int ev_range_high, "none"
    ] (fun s -> failwith s) "");
  gen ()
