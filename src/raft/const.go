/**
 * @File: const.go
 * @Author: 数据体系-xiesenxin
 * @Date：2023/6/25
 */
package raft

const (
	HearbeatTimeout = 1000
)

type Role string

const (
	RoleFollower  = "follower"
	RoleCandidate = "candidate"
	RoleLeader    = "leader"
)
