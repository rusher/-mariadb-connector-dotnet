using MariadbConnector.client.util;

namespace MariadbConnector.client.impl;

public class LruPrepareCache : IPrepareCache
{
    private readonly Dictionary<string, Node> _cacheWithData;
    private readonly int _capacity;
    private readonly IClient _client;
    private Node _head;
    private Node _tail;

    public LruPrepareCache(uint capacity, IClient client)
    {
        _capacity = (int)capacity;
        _cacheWithData = new Dictionary<string, Node>();
        _client = client;
    }

    public IPrepare Get(string key, MariaDbCommand dbCommand)
    {
        IPrepare? value = null;
        Node temp;

        if (_cacheWithData.TryGetValue(key, out temp))
        {
            value = temp.Value;
            // Now Move this Node to First location because it is most Recent Used
            MoveTo(temp);
            if (dbCommand != null) value.IncrementUse(dbCommand);
        }

        return value;
    }

    public IPrepare Put(string key, IPrepare value, MariaDbCommand dbCommand)
    {
        // case 1 :- Key does't exist in Dictionary
        if (_cacheWithData.ContainsKey(key))
        {
            var node = _cacheWithData[key];
            MoveTo(node);
            if (dbCommand != null) node.Value.IncrementUse(dbCommand);

            return node.Value;
        }
        else
        {
            //So we have to add new key.
            var node = new Node { Key = key, Value = value };
            if (_head == null)
            {
                _tail = _head = node;
                // tail.Previous = head.Next = node;
            }
            else
            {
                // Add new Item on first location
                node.Next = _head;
                _head.Previous = node;
                _head = node;
            }

            _cacheWithData.Add(key, node);
            if (_cacheWithData.Count > _capacity)
            {
                // it's mean we need to removed last element from the cache
                _tail.Value.UnCache(_client);
                _cacheWithData.Remove(_tail.Key);
                if (_tail.Previous != null)
                    _tail.Previous.Next = null;
                _tail = _tail.Previous;
            }

            return value;
        }
    }

    public void Reset()
    {
        _cacheWithData.Clear();
        _head = null;
        _tail = null;
    }

    private void MoveTo(Node node)
    {
        if (_head == node) return; // No Need to Perform any thing
        if (node == _tail)
        {
            // Last element 
            // so move last element to first location
            var tempTail = _tail;
            _tail.Previous.Next = null;
            _tail = _tail.Previous;
        }
        else
        {
            // Middle Node
            var previous = node.Previous;
            previous.Next = node.Next;
            node.Next.Previous = previous;
        }

        // Moved node on to the first element
        _head.Previous = node;
        node.Previous = null;
        node.Next = _head;
        _head = node;
    }
}

public class Node
{
    public IPrepare Value { get; set; }
    public string Key { get; set; }
    public Node Previous { get; set; }
    public Node Next { get; set; }
}