import os
import pwd
import grp


def drop_privileges(user: str) -> None:
    """
    Drop user privileges.

    :params str user: Username to which we should change permissions
    """
    new_user = pwd.getpwnam(user)
    if new_user.pw_uid == os.getuid():
        return
    new_gids = [new_user.pw_gid]
    new_gids += [group.gr_gid for group in grp.getgrall() if new_user.pw_name in group.gr_mem]
    os.setgroups(new_gids[:os.NGROUPS_MAX])
    os.setgid(new_user[0])
    os.setuid(new_user.pw_uid)
    os.environ["HOME"] = new_user.pw_dir
