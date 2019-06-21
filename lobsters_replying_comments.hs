
query read_ribbons comments stories votes = do
    ribbon@Ribbon {rstory_id, is_following, r_user_id} <- read_ribbons
    comment@Comment { c_id
                    , c_is_deleted
                    , c_is_moderated
                    , parent_comment_id
                    , comment_author_id
                    } <- (lookup `on` story_id) rstory_id comments
    story@Story {s_user_id, s_upvotes, s_downvotes} <-
        (lookup `on` id) rstory_id stories
    guard $ is_following == 1
    guard $ not c_is_deleted
    guard $ not c_is_moderated
    guard $ c_upvotes - c_downvotes >= 0
    guard $ s_upvotes - s_downvotes >= 0
    (p_author_id) <-
        case (lookup `on` id) parent_comment comments of
            [] -> pure Nothing
            res -> do
                parent_comment@Comment {p_user_id, p_comments_id} <- res
                guard $
                    p_user_id == r_user_id ||
                    (isNull p_user_id && r_user_id == s_user_id)
                guard $
                    isNUll p_comments_id ||
                    upvotes parent_comment - downvotes parent_comment >= 0
                pure $ Just $ user_id parent_comment
    let is_unread = updated_at ribbon > created_at comments
    vote <- (findOne `on` user_id) r_user_id votes
    return
        ( r_user_id
        , c_id
        , story_id ribbon
        , parent_comment_id
        , p_author_id
        , comment_author_id
        , s_user_id
        , is_unread
        , vote_value vote
        , reason vote)
