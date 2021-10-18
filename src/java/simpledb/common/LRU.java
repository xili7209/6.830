package simpledb.common;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.Map;

public class LRU {
    private Map<PageId, DLinkedNode> cache = new HashMap<>();
    private DLinkedNode head, tail;

    public class DLinkedNode {
        PageId key;
        Page value;
        DLinkedNode prev;
        DLinkedNode next;
        public DLinkedNode() {}
        public DLinkedNode(PageId _key, Page _value) {key = _key; value = _value;}
        public Page getPageVal(){return  value;}
    }
    public LRU(int capacity) {

        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        tail.prev = head;
    }
    private void setHead(DLinkedNode node){
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }
    private void moveToHead(DLinkedNode node){
        removeNode(node);
        setHead(node);
    }
    private void removeNode(DLinkedNode node){
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    public Page get(PageId key) {
        DLinkedNode node = cache.get(key);
        if(node==null) return null;
        moveToHead(node);
        return node.value;
    }
    public void put(PageId key, Page value) {
        DLinkedNode node = cache.get(key);
        if(node!=null){
            node.value = value;
            moveToHead(node);
        }else{
            node = new DLinkedNode(key,value);
            setHead(node);
            cache.put(key,node);

        }
    }

    public DLinkedNode evictNode(){
        return tail.prev;
    }

    public void remove(PageId pageId){
        DLinkedNode dLinkedNode = cache.get(pageId);
        removeNode(dLinkedNode);
        cache.remove(pageId);

    }

    public boolean containsKey(PageId pageId){
        return cache.containsKey(pageId);
    }

    public int getSize(){
        return cache.size();
    }



}
