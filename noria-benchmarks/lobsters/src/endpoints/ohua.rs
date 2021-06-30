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
        acting_as: Option<UserId>,
        uid: UserId,
    ) -> Box<dyn Future<Item = (my::Conn, bool), Error = my::error::Error> + Send>
    where
        F: 'static + Future<Item = my::Conn, Error = my::error::Error> + Send,
    {
        crate::endpoints::noria::user::handle(c, acting_as, uid)
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

        crate::endpoints::noria::frontpage::handle(c, acting_as)
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
        crate::endpoints::noria::comments::handle(c, acting_as)
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
        crate::endpoints::noria::recent::handle(c, acting_as)
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
        crate::endpoints::noria::story::handle(c, acting_as, simulate_shards, id)
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
        crate::endpoints::noria::story_vote::handle(c, acting_as, story, v)
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
        crate::endpoints::noria::comment_vote::handle(c, acting_as, comment, v)
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
        crate::endpoints::noria::submit::handle(c, acting_as, id, title)
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
        crate::endpoints::noria::comment::handle(c, acting_as, id, story, parent)
    }
}

pub(crate) fn notifications(
    c: my::Conn,
    uid: u32,
) -> impl Future<Item = my::Conn, Error = my::error::Error> {
    c.drop_exec(
        "SELECT BOUNDARY_notifications.notifications
         FROM BOUNDARY_notifications
         WHERE BOUNDARY_notifications.user_id = ?",
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
