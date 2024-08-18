---
title: CompressGtidSets
---

# 20230609_CompressGtidSets

- Feature: CompressGtidSets
- Start Date: 2023-06-09
- Authors: @gerayking
- RFC PR: https://github.com/wescale/pull/103
- WeSQL WeScale Issue: https://github.com/wescale/issues/37

# BackGround

For further related information, please refer to [20230414_ReadAfterWrite](https://github.com/apecloud/wescale/blob/vitess-release-16.0-dev/doc/design-docs/RFCS/20230414_ReadAfterWrite.md)

## Why is gtidSets discontinued?

In the context of WeSQL WeScale, there are two levels of Read After Write (RAW), namely instance and session levels. Currently, the session-level gtid is stored as the latest gtid, while the instance-level solution involves maintaining a gtidSets in the memory of vtgate and incorporating it into the SQL to be executed, such as `select`. This article focuses only on the instance-level solution. When multiple vtgates are in play, for instance, vtgate1 and vtgate2, and SQL is cross-accessing these vtgates, numerous internal fragmentations will occur in the gtidSet maintained by vtgate1 and vtgate2. The following images depict a scenario involving 2 vtgates.

![multivtgate](images/multivtgate.jpg)
vtgate1:
`gtidset=df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:25081:25083:25085:25087`

vtgate2:
`gtidset=df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:25709-25080:25082:25084:25086:25088`

## Why need Compress GtidSets?

For the relevant issue, please refer to [#37](https://github.com/apecloud/wescale/issues/37)

## Summary

The proposed change aims to compress the Gtid set to prevent SQL commands from becoming excessively lengthy.

# Technical design

## When we need CompressGtidSet?

We made some tradeoffs and finally decided to perform compression after the return of the heartbeat packet. Moreover, there is an optimization that triggers compression when the length of the gtidset exceeds a certain threshold.


## Design Details

Each VTGate is responsible for maintaining the gtidset, which represents its up-to-date understanding of the existing gtids for that specific instance of VTGate. During execution stage, the VTGate need to use the gtidset to ensure read-after-write-consistency.

To compress the GTID set, we need to acquire the GTID set from each MySQL instance and merge it with the gtidset maintained within vtgate.

### Step 1: Get GTID set from tablet 

Within tablet_health_check, vtgate sends a heartbeat packet to vttablet. The response from the heartbeat packet carries the gtidset of the current vttablet and its corresponding MySQL instance. Hence, the Gtid set of each tablet can be derived from the response.

### Step 2: Compress GtisSet with lastseengtid

+ Get the intersection of all mysql GTID sets.
    Example:
    ```yaml
    tablet1: 1~5,7~10
    tablet2: 1~4,8~12
    tablet3: 1~4,7~9
     
    inttersectionSet: 1-4:8-9
    ```
+ Merge intersectionSet and lastSeenGtid.
    The lastSeenGtid refers to the gtid sets maintained by vtagte. In this section, we utilize the mysql56GtidSet.Union function for implementation.
    + First we cut off the part larger than lastSeenGtid in inttersectionSet.
    Example:
        ```yaml
        lastSeenGtid: 1~3,5~6
        inttersectionSet after cut off: 1~4
        ```
    + Then, calculate the union of lastSeenGtid andthe remaining portion after truncation.
     ```
     final result: 1~6
     ```
    
### Conclusion

After the return of the heartbeat packet, we compress the gtidset within the vtgate. In a scenario with multiple vtgates, we ensured read-after-write (RAW) consistency and prevented the SQL statement from becoming too lengthy. However, the mechanism is still reliant on the return of the heartbeat packet from all MySQL instances.

# Future Works

Given that the current mechanism still depends on the return of the heartbeat packet, it will be necessary to actively monitor the length of the gtidset and proactively synchronize the gtidset of each MySQL instance in the future.

# Reference

- 20230414_ReadAfterWrite: [https://github.com/wesql/wescale/blob/main/doc/design/20230414_ReadAfterWrite.md](https://github.com/wesql/wescale/blob/main/doc/design/20230414_ReadAfterWrite.md)
