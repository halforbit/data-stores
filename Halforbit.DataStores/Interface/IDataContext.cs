﻿using System;

namespace Halforbit.DataStores
{
    public interface IDataContext
    {
        IDataStore<TKey, TValue> Get<TKey, TValue>(
            Func<INeedsIntegration, IDataStoreDescription<TKey, TValue>> getDataStoreDescription);

        IDataStore<TValue> Get<TValue>(
            Func<INeedsIntegration, IDataStoreDescription<TValue>> getDataStoreDescription);
    }
}
