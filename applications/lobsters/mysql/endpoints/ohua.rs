use futures::{Future, failed};
use my;
use my::prelude::*;
use trawler::{Vote, UserId, StoryId, CommentId};

macro_rules! unimplemented {
    ($s:expr) => {
        Box::new(
            failed(my::error::Error::Other(format_err!("Endpoint `{}` is not implemented for this backend (ohua)", $s)))
        )
        }
}

pub mod user {
    use super::*;
    pub(crate) fn handle<F>(
        c: F,
        _acting_as: Option<UserId>,
        uid: UserId,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        unimplemented!("user")
    }
}

pub mod frontpage {
    use super::*;
    pub(crate) fn handle<F>(
        c: F,
        acting_as: Option<UserId>,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        unimplemented!("user")
    }
}

pub mod comments {
    use super::*;
    pub(crate) fn handle<F>(
        c: F,
        acting_as: Option<UserId>,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        unimplemented!("comments")
    }
}

pub mod recent {
    use super::*;
    pub(crate) fn handle<F>(
        c: F,
        acting_as: Option<UserId>,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        unimplemented!("recent")
    }
}

pub mod story {
    use super::*;
    pub(crate) fn handle<F>(
        c: F,
        acting_as: Option<UserId>,
        simulate_shards: Option<u32>,
        id: StoryId,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        unimplemented!("story")
    }
}

pub mod story_vote {
    use super::*;
    pub(crate) fn handle<F>(
        c: F,
        acting_as: Option<UserId>,
        story: StoryId,
        v: Vote,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        unimplemented!("story_vote")
    }
}
pub mod comment_vote {
    use super::*;
    pub(crate) fn handle<F>(
        c: F,
        acting_as: Option<UserId>,
        comment: StoryId,
        v: Vote,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        unimplemented!("comment_vote")
    }
}
pub mod submit {
    use super::*;
    pub(crate) fn handle<F>(
        c: F,
        acting_as: Option<UserId>,
        id: StoryId,
        title: String,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        unimplemented!("submit")
    }
}
pub mod comment {
    use super::*;
    pub(crate) fn handle<F>(
        c: F,
        acting_as: Option<UserId>,
        id: CommentId,
        story: StoryId,
        parent: Option<CommentId>,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        unimplemented!("comment")
    }
}

pub(crate) fn notifications(
    c: my::Conn,
    uid: u32,
) -> impl Future<Item = my::Conn, Error = my::error::Error> {
    c.drop_exec(
        "SELECT COUNT(*) \
         FROM `replying_comments_for_count`
                     WHERE `replying_comments_for_count`.`user_id` = ? \
                     GROUP BY `replying_comments_for_count`.`user_id` \
                     ",
        (uid,),
    )
        .and_then(move |c| {
            c.drop_exec(
                "SELECT `keystores`.* \
                 FROM `keystores` \
                 WHERE `keystores`.`key` = ?",
                (format!("user:{}:unread_messages", uid),),
            )
        })
}
