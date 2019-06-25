fn click_ana(start_cat: Category,
             end_cat: Category,
             clicks: &mut Vec<(UID, Category, Time)>) -> i32 {
    let stream = HashMap::new();
    let distances = for (uid, cat, _) in clicks {
        let prev = stream.get(uid);
        if cat == start_cat {
            stream.insert(uid, 0);
            None
        } else if prev != -1 {
            if cat == end_cat {
                stream.insert(uid, -1);
                Some(prev)
            } else {
                stream.insert(uid, prev + 1);
                None
            }
        }
        None
    }
    average(drop_none(distances))
}


fn partial_click_ana(start_cat: Category, end_cat: Category, clicks: Stream<(UID, Category, Time)>) -> i32 {
    use iseq::Action;
    let click_streams = group_by::<0>(clicks);
    Stream::concat(for click_stream in click_streams {
        let sequences = iseq::Seq::new();
        for (_, cat, time) in stream {
            let ev = if cat == start_cat {
                Action::Open(time)
            } else if cat == end_cat {
                Action::Close(time)
            } else {
                Action::Insert(time)
            }
            sequences.apply(ev);
        }
        sequences.complete_intervals().map(Interval::len)
    })
}
