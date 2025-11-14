#!/bin/bash

echo "==========================================="
echo "  Big Data Stack Health Check"
echo "==========================================="
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Java
echo -e "${YELLOW}[1/5] Checking Java...${NC}"
if java -version 2>&1 >/dev/null; then
    java -version 2>&1 | head -1
    echo -e "${GREEN}✓ Java is installed${NC}"
else
    echo -e "${RED}✗ Java not found${NC}"
fi
echo ""

# Check Hadoop/HDFS
echo -e "${YELLOW}[2/5] Checking Hadoop/HDFS...${NC}"
if command -v hadoop &> /dev/null; then
    hadoop version | head -1
    
    # Check if HDFS is running
    if jps | grep -q "NameNode"; then
        echo -e "${GREEN}✓ NameNode is running${NC}"
        
        # Test HDFS command
        if hdfs dfs -ls / &> /dev/null; then
            echo -e "${GREEN}✓ HDFS is accessible${NC}"
        else
            echo -e "${RED}✗ HDFS not accessible${NC}"
        fi
    else
        echo -e "${RED}✗ NameNode not running (run: start-dfs.sh)${NC}"
    fi
else
    echo -e "${RED}✗ Hadoop not found${NC}"
fi
echo ""

# Check Sqoop
echo -e "${YELLOW}[3/5] Checking Sqoop...${NC}"
if command -v sqoop &> /dev/null; then
    sqoop version 2>&1 | grep "Sqoop" | head -1
    echo -e "${GREEN}✓ Sqoop is installed${NC}"
else
    echo -e "${RED}✗ Sqoop not found${NC}"
fi
echo ""

# Check Flume
echo -e "${YELLOW}[4/5] Checking Flume...${NC}"
if command -v flume-ng &> /dev/null; then
    flume-ng version 2>&1 | head -1
    echo -e "${GREEN}✓ Flume is installed${NC}"
else
    echo -e "${RED}✗ Flume not found${NC}"
fi
echo ""

# Check Kafka
echo -e "${YELLOW}[5/5] Checking Kafka...${NC}"
if command -v kafka-topics.sh &> /dev/null; then
    # Check if Kafka is running
    if jps | grep -q "Kafka"; then
        echo -e "${GREEN}✓ Kafka server is running${NC}"
        
        # Test Kafka connection
        if kafka-topics.sh --list --bootstrap-server localhost:9092 &> /dev/null; then
            echo -e "${GREEN}✓ Kafka is accessible${NC}"
            echo "Topics:"
            kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null
        else
            echo -e "${RED}✗ Kafka not accessible${NC}"
        fi
    else
        echo -e "${RED}✗ Kafka not running (run: ~/start-kafka.sh or start manually)${NC}"
    fi
    
    # Check Zookeeper
    if jps | grep -q "QuorumPeerMain"; then
        echo -e "${GREEN}✓ Zookeeper is running${NC}"
    else
        echo -e "${RED}✗ Zookeeper not running${NC}"
    fi
else
    echo -e "${RED}✗ Kafka not found${NC}"
fi
echo ""

# Summary of running processes
echo "==========================================="
echo "  Running Java Processes (jps)"
echo "==========================================="
jps
echo ""

echo "==========================================="
echo "  Summary"
echo "==========================================="
echo "Expected processes for full stack:"
echo "  - NameNode, DataNode, SecondaryNameNode (Hadoop)"
echo "  - QuorumPeerMain (Zookeeper)"
echo "  - Kafka (Kafka Server)"
echo ""