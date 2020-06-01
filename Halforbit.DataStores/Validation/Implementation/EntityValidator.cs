using Halforbit.ObjectTools.Extensions;
using Halforbit.ObjectTools.ObjectStringMap.Interface;
using System;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public class EntityValidator<TKey, TValue> : ValidatorBase<TKey, TValue>
    {
        readonly Func<TValue, TKey> _getKeyFromEntity;

        public EntityValidator(Func<TValue, TKey> getKeyFromEntity = default)
        {
            _getKeyFromEntity = getKeyFromEntity;
        }

        protected override async Task<ValidationErrors> ValidatePut(
            TKey key, 
            TValue value, 
            IStringMap<TKey> keyMap, 
            ValidationErrors errors)
        {
            return errors
                .If(key.IsDefaultValue(),
                    new ValidationError("Key must be provided.", true))
                .If(value.IsDefaultValue(),
                    new ValidationError("Value must be provided.", true))
                .If(_getKeyFromEntity != null && keyMap.Map(key) != keyMap.Map(_getKeyFromEntity(value)),
                    new ValidationError("Key does not match key in entity.", true));
        }
    }
}
