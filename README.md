# bskyAllBlock
Silly project to add all of bsky to a moderation list

This will move through a few modes of operation as time goes on. Right now we are in Phase I (Firehose).

Phase I -- Firehose
Scrape the firehose feed and block all DID's that are referenced in posts, reposts, and likes. Right now we are rate limited at an absolute maximum of ~11,600 accounts per day, but in reality this number is closer to half that. Bluesky has ~1.1m active users at the time of writing this. If following 80/20 rule, we will have blocked 20% of the active users in about 38 days, cutting out 80% of the posts and content on the site. At this point I expect the Firehose method to slow down dramatically.

Right now I am only browsing the firehose when I'm not rate limited. This means that posters who get engagement/post at times that are out of sync with my rate limits get missed. This will hopefully correct itself over time. Eventually I'll reach a point where I don't have 1600 new accounts to block in an hour, and firehose method can run continuously at that point.

TODO: Let firehose run continously, and add accounts seen to a block queue to be run when the rate limit is over.

Phase II: List Assimilation
Once we've run through the low hanging fruit in firehose, the next step will be to start making use of existing lists to catch inactive accounts. Go through each user already on the list, fetch THEIR moderation lists, and scour them for DID's that are not on our blocklist already.

There's no good way to tell how long this phase will take. It may take months to complete. Firehose method will also still be running during this, which may also slow it down as that method eats request credits.

Phase III: Ultra Instinct Blockenheimer
Once we've exhausted most content in Phase II, the next step will be to start scouring the follower/following lists of accounts that we have listed. We can also take new accounts found this way and pass them through Phase II in case these new findings also have lists.

Limitations:

Bsky has about 6 million total registered accounts at the time of writing this. If by some miracle we can block at the maximum rate every day continuously (not possible), we would block all 6M in 518 days -- a bit under 2 years, not accounting for new accounts made during this time.

Orphaned accounts are not discoverable for blocking (accounts that never post, repost, or like content, aren't on any lists of active accounts, do not follow/are followed by any listed accounts... basically, pure lurk accounts.)
