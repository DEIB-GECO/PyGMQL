Remote data management
======================

PyGMQL can be used in two different ways. The first one (and the most intuitive and classical one)
is to use it like any other computational library.

PyGMQL also manages the execution through a remote server (or cluster). In order to use this
feature the user needs to login to the remote service before::

    import gmql as gl

    gl.login("username", "password")


The second way is to make the library interact with a remote server. In order to do that you need to
create an instance of a *RemoteManager*.

.. currentmodule:: gmql.RemoteConnection.RemoteManager
..  autoclass:: RemoteManager
    :members:

