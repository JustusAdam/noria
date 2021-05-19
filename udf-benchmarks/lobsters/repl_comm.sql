CREATE VIEW replying_comments_original AS
	SELECT `read_ribbons`.`user_id`, `read_ribbons`.`story_id`, `comments`.`id`,
	    `comments`.`upvotes` - `comments`.`downvotes` AS saldo,
	    `parent_comments`.`upvotes` - `parent_comments`.`downvotes` AS psaldo
	FROM `read_ribbons`
	JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`)
	JOIN `comments` ON (`comments`.`story_id` = `read_ribbons`.`story_id`)
	JOIN `parent_comments`
	ON (`comments`.`parent_comment_id` = `parent_comments`.`id`)
	WHERE `read_ribbons`.`is_following` = 1
	AND `comments`.`user_id` <> `read_ribbons`.`user_id`
	AND `comments`.`is_deleted` = 0
	AND `comments`.`is_moderated` = 0
	AND saldo >= 0
	AND (
     (
     	`stories`.`user_id` = `read_ribbons`.`user_id`
     	AND
     	psaldo >= 0
     )
     );
