using System.Collections;
using System.Data.Common;

namespace MariadbConnector.utils;

public class MariaDbParameterCollection : DbParameterCollection
{
    private int _count;
    private DbParameter[] elementData;

    public MariaDbParameterCollection(int defaultSize)
    {
        elementData = new DbParameter[defaultSize];
        _count = 0;
    }

    public MariaDbParameterCollection()
    {
        elementData = new DbParameter[10];
        _count = 0;
    }

    public override int Count => _count;

    public override bool IsFixedSize { get; }
    public override bool IsReadOnly { get; }
    public override bool IsSynchronized { get; }
    public override object SyncRoot { get; }


    protected override DbParameter GetParameter(int index)
    {
        if (index >= _count)
            throw new IndexOutOfRangeException($"wrong index {index} Count:{_count}");
        return elementData[index];
    }

    protected override void SetParameter(int index, DbParameter element)
    {
        if (elementData.Length <= index) Grow(index + 1);
        elementData[index] = element;
        if (index >= _count) _count = index + 1;
    }

    protected override void SetParameter(string name, DbParameter element)
    {
        throw new NotImplementedException();
    }

    private void Grow(int minLength)
    {
        var currLength = elementData.Length;
        var newLength = Math.Max(currLength + (currLength >> 1), minLength);
        var tmpElementData = new DbParameter[newLength];
        Array.Copy(elementData, 0, tmpElementData, 0, currLength);
        elementData = tmpElementData;
    }

    public override void CopyTo(Array array, int index)
    {
        Array.Copy(elementData, 0, array, index, _count);
    }

    public override int Add(object value)
    {
        if (value is DbParameter dbParameter)
        {
            SetParameter(_count, dbParameter);
            return _count - 1;
        }

        var param = new MariaDbParameter();
        param.Value = value;
        SetParameter(_count, param);
        return _count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var val in values) Add(val);
    }

    public override void Clear()
    {
        elementData = new DbParameter[10];
        _count = 0;
    }

    public override bool Contains(object value)
    {
        for (var i = 0; i < _count; i++)
            if (Equals(value, elementData[i]))
                return true;
        return false;
    }

    public override bool Contains(string value)
    {
        throw new NotImplementedException();
    }

    public override IEnumerator GetEnumerator()
    {
        var tmpElementData = new DbParameter[_count];
        Array.Copy(elementData, 0, tmpElementData, 0, _count);
        return tmpElementData.GetEnumerator();
    }

    protected override DbParameter GetParameter(string parameterName)
    {
        throw new NotImplementedException();
    }

    public override int IndexOf(object value)
    {
        for (var i = 0; i < _count; i++)
            if (Equals(value, elementData[i]))
                return i;
        return -1;
    }

    public override int IndexOf(string parameterName)
    {
        throw new NotImplementedException();
    }

    public override void Insert(int index, object value)
    {
        if (value is DbParameter dbParameter) SetParameter(index, dbParameter);

        var param = new MariaDbParameter();
        param.Value = value;
        SetParameter(index, param);
    }

    public override void Remove(object value)
    {
        for (var i = 0; i < _count; i++)
            if (Equals(value, elementData[i]))
                elementData[i] = null;
    }

    public override void RemoveAt(int index)
    {
        elementData[index] = null;
    }

    public override void RemoveAt(string parameterName)
    {
        throw new NotImplementedException();
    }

    public override object InitializeLifetimeService()
    {
        return base.InitializeLifetimeService();
    }
}