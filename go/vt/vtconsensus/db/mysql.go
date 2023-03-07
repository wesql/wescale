/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
ApeCloud MySQL db interface.
*/
package db

import (
	"errors"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtconsensus/config"
	"vitess.io/vitess/go/vt/vtconsensus/inst"
)

var (
	configFilePath       string
	enableHeartbeatCheck bool
	dbUser               string
	dbPasswd             string
	// ErrGroupBackoffError is either the transient error or network partition from the group
	ErrGroupBackoffError = errors.New("group backoff error")
	// ErrGroupInactive is the error when mysql group is inactive unexpectedly
	ErrConsensusNoLeader = errors.New("consensus no leader")
	// ErrInvalidInstance is the error when the instance key has empty hostname
	ErrInvalidInstance = errors.New("invalid mysql instance key")
)

func init() {
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		fs.StringVar(&configFilePath, "db_config", "", "Full path to db config file that will be used by VTConsensus.")
		fs.BoolVar(&enableHeartbeatCheck, "enable_heartbeat_check", false, "Enable heartbeat checking, set together with --group_heartbeat_threshold.")
		fs.StringVar(&dbUser, "db_username", "root", "ApeCloud MySQL access username, --db_username, default root.")
		fs.StringVar(&dbPasswd, "db_password", "", "ApeCloud MySQL access password, --db_passwd, default empty.")
	})
}

// Agent is used by vtconsensus to interact with Mysql
type Agent interface {
	// FetchConsensusLocalView fetches consensus local view related information
	FetchConsensusLocalView(alias string, instanceKey *inst.InstanceKey,
		globalView *ConsensusGlobalView) (*ConsensusGlobalView, *ConsensusLocalView, error)
	// FetchConsensusGlobalView fetches consensus global view
	FetchConsensusGlobalView(globalView *ConsensusGlobalView)
}

// MemberState is member state
type MemberState int

// MemberRole is member role
type MemberRole int

const (
	UNKNOWNSTATE MemberState = iota
	OFFLINE
	UNREACHABLE
	RECOVERING
	ONLINE
	ERROR
)

const (
	UNKNOWNROLE MemberRole = iota
	SECONDARY              // Apecloud MySQL Follower role is SECONDARY
	PRIMARY                // Apecloud MySQL Leader role is PRIMARY
)

type ConsensusRole int

const (
	UNKNOWNCONSENSUSROLE ConsensusRole = iota
	FOLLOWER
	LEADER
	LOGGER
	LEARNER
	CANDIDATE
)

// ConsensusLocalView represents a view we get from information_schema.wesql_cluster_local
type ConsensusLocalView struct {
	TabletAlias    string
	ServerID       int
	CurrentLeader  string
	LeaderHostName string
	LeaderHostPort int
	MySQLHost      string
	MySQLPort      int
	Role           ConsensusRole
	IsRW           int
}

// ConsensusMember is instance info by wesql_cluster_global and wesql_cluster_local
type ConsensusMember struct {
	ServerID           int
	MySQLHost          string
	MySQLPort          int
	Role               ConsensusRole
	ForceSync          int
	ElectionWeight     int
	LearnerSource      string
	Connected          bool
	HeartbeatStaleness int
}

// ConsensusGlobalView is an instance's view for the apecloud mysql wesql_cluster_global
type ConsensusGlobalView struct {
	LeaderMySQLHost string
	LeaderMySQLPort int
	LeaderServerID  int
	ResolvedMember  map[inst.InstanceKey]ConsensusMember
	LocalView       []*ConsensusLocalView
}

// SQLAgentImpl implements Agent
type SQLAgentImpl struct {
	config          *config.Configuration
	enableHeartbeat bool
	dbUserName      string
	dbPassword      string
}

func NewConsensusLocalView(tabletAlias string, serverID int, currentLeader string,
	leaderHostName string, leaderHostPort int, mySQLHost string,
	mySQLPort int, role ConsensusRole, isRW int) *ConsensusLocalView {
	return &ConsensusLocalView{TabletAlias: tabletAlias, ServerID: serverID,
		CurrentLeader: currentLeader, LeaderHostName: leaderHostName,
		LeaderHostPort: leaderHostPort, MySQLHost: mySQLHost, MySQLPort: mySQLPort,
		Role: role, IsRW: isRW}
}

func NewConsensusMember(serverID int, mySQLHost string,
	mySQLPort int, role ConsensusRole, forceSync int, electionWeight int,
	learnerSource string, connected bool) *ConsensusMember {
	return &ConsensusMember{ServerID: serverID, MySQLHost: mySQLHost,
		MySQLPort: mySQLPort, Role: role, ForceSync: forceSync,
		ElectionWeight: electionWeight, LearnerSource: learnerSource, Connected: connected}
}

// NewConsensusGlobalView creates a new ConsensusGlobalView
func NewConsensusGlobalView() *ConsensusGlobalView {
	return &ConsensusGlobalView{}
}

// NewVTConsensusSqlAgent creates a SQLAgentImpl
func NewVTConsensusSqlAgent() *SQLAgentImpl {
	var conf *config.Configuration
	if (configFilePath) != "" {
		log.Infof("use config from %v", configFilePath)
		conf = config.ForceRead(configFilePath)
	} else {
		log.Warningf("use default config")
		conf = config.Config
	}
	agent := &SQLAgentImpl{
		config:          conf,
		enableHeartbeat: enableHeartbeatCheck,
		dbUserName:      dbUser,
		dbPassword:      dbPasswd,
	}
	return agent
}

// heartbeatCheck returns heartbeat check freshness result
func (agent *SQLAgentImpl) heartbeatCheck(instanceKey *inst.InstanceKey) (int, error) {
	query := `select timestampdiff(SECOND, from_unixtime(truncate(ts * 0.000000001, 0)), NOW()) as diff from _vt.heartbeat;`
	var result int
	err := fetchInstance(instanceKey, query, agent.dbUserName, agent.dbPassword, func(m sqlutils.RowMap) error {
		result = m.GetInt("diff")
		return nil
	})
	return result, err
}

// FetchConsensusLocalView implements Agent interface
func (agent *SQLAgentImpl) FetchConsensusLocalView(alias string, instanceKey *inst.InstanceKey,
	globalView *ConsensusGlobalView) (*ConsensusGlobalView, *ConsensusLocalView, error) {
	var leaderHostname string
	var leaderHostPort int
	var localView *ConsensusLocalView

	if globalView == nil {
		globalView = NewConsensusGlobalView()
	}

	query := `select server_id, current_leader, 
        left(current_leader, locate(':', current_leader) -1) as leader_hostname, 
    	@@port as leader_port, 
    	case role when 'Leader' then 2 when 'Follower' then 1 else 0 end as role, 
        case server_ready_for_rw when 'NO' then 0  when 'YES' then 1 end as isrw 
	from information_schema.wesql_cluster_local;`

	err := fetchInstance(instanceKey, query, agent.dbUserName, agent.dbPassword, func(m sqlutils.RowMap) error {
		leaderHostname = m.GetString("leader_hostname")
		leaderHostPort = m.GetInt("leader_port")
		localView = NewConsensusLocalView(
			alias,
			m.GetInt("server_id"),
			m.GetString("current_leader"),
			leaderHostname,
			leaderHostPort,
			instanceKey.Hostname,
			instanceKey.Port,
			ConsensusRole(m.GetInt("role")),
			m.GetInt("isrw"))
		globalView.LocalView = append(globalView.LocalView, localView)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return globalView, localView, nil
}

// FetchConsensusGlobalView returns the view of wesql_cluster_global
func (agent *SQLAgentImpl) FetchConsensusGlobalView(globalView *ConsensusGlobalView) {
	query := `select g.server_id,          
    left(g.ip_port, locate(':', g.ip_port) -1) as hostname,      
    @@port as port,     case g.role when 'Leader' then 2 when 'Follower' then 1 else 0 end as role,     
    case force_sync when 'YES' then 1 when 'NO' then 0 end as force_sync,      
    election_weight,learner_source, 
    case connected when 'YES' then true when 'NO' then false end as connected
    from information_schema.wesql_cluster_global g, information_schema.wesql_cluster_health h 
    where g.server_id = h.server_id;`

	if globalView.LeaderMySQLHost == "" || globalView.LeaderMySQLPort == 0 {
		return
	}
	leaderInstance := inst.InstanceKey{
		Hostname: globalView.LeaderMySQLHost,
		Port:     globalView.LeaderMySQLPort,
	}
	mk := make(map[inst.InstanceKey]ConsensusMember)

	err := fetchInstance(&leaderInstance, query, agent.dbUserName, agent.dbPassword, func(m sqlutils.RowMap) error {
		var realHost string
		var realPort int
		hostName := m.GetString("hostname")
		hostPort := m.GetInt("port")
		serverId := m.GetInt("server_id")

		// ConsensusLocalView record real host:port, because from instanceKey.
		for _, lv := range globalView.LocalView {
			if lv.ServerID == serverId {
				realHost = lv.MySQLHost
				realPort = lv.MySQLPort
				break
			}
		}
		consensusMember := NewConsensusMember(
			m.GetInt("server_id"),
			hostName,
			hostPort,
			ConsensusRole(m.GetInt("role")),
			m.GetInt("force_sync"),
			m.GetInt("election_weight"),
			m.GetString("learner_source"),
			m.GetBool("connected"))
		curInstance := inst.InstanceKey{
			Hostname: realHost,
			Port:     realPort,
		}
		mk[curInstance] = *consensusMember
		return nil
	})
	if err != nil {
		return
	}
	globalView.ResolvedMember = mk
}

// GetLeaderHostPort returns the host and port of Leader
func (view *ConsensusGlobalView) GetLeaderHostPort(local *ConsensusLocalView) (string, int) {
	return local.LeaderHostName, local.LeaderHostPort
}

// fetchInstance fetches result from mysql
func fetchInstance(instanceKey *inst.InstanceKey, query string, userName string, passwd string, onRow func(sqlutils.RowMap) error) error {
	if err := verifyInstance(instanceKey); err != nil {
		return err
	}
	sqlDb, err := OpenDiscovery(instanceKey.Hostname, instanceKey.Port, userName, passwd)
	if err != nil {
		return err
	}
	return sqlutils.QueryRowsMap(sqlDb, query, onRow)
}

// The hostname and port can be empty if a tablet crashed and did not populate them in
// the topo server. We treat them as if the host is unreachable when we calculate the
// quorum for the shard.
func verifyInstance(instanceKey *inst.InstanceKey) error {
	if instanceKey.Hostname == "" || instanceKey.Port == 0 {
		return ErrInvalidInstance
	}
	return nil
}

// CreateInstanceKey returns an InstanceKey
func (view *ConsensusGlobalView) CreateInstanceKey(member *ConsensusMember) inst.InstanceKey {
	return inst.InstanceKey{
		Hostname: member.MySQLHost,
		Port:     member.MySQLPort,
	}
}
