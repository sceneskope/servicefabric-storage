using System;

namespace SceneSkope.ServiceFabric.Storage
{
    public partial class ReliableListKey : IComparable<ReliableListKey>
    {
        public int CompareTo(ReliableListKey other)
        {
            var result = Key.CompareTo(other.Key);
            if (result != 0) return result;
            return Id.CompareTo(other.Id);
        }
    }
}
