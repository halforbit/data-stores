using Halforbit.ObjectTools.ObjectStringMap.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Halforbit.DataStores
{
    public abstract class ValidatorBase<TKey, TValue> :
        IValidator<TKey, TValue>
    {
        readonly IReadOnlyList<ValidatorBase<TKey, TValue>> _prerequisites;

        public ValidatorBase(
            params ValidatorBase<TKey, TValue>[] prerequisites)
        {
            _prerequisites = prerequisites;
        }

        public async Task<ValidationErrors> ValidateDelete(
            TKey key,
            IStringMap<TKey> keyMap)
        {
            var errors = ValidationErrors.Empty;

            if (_prerequisites != null)
            {
                foreach(var prerequisite in _prerequisites)
                {
                    errors = errors.With(await prerequisite.ValidateDelete(key, keyMap).ConfigureAwait(false));

                    if(errors.Any(e => e.IsFatal))
                    {
                        return errors;
                    }
                }
            }

            return errors.With(await ValidateDelete(key, keyMap, errors).ConfigureAwait(false));
        }

        public async Task<ValidationErrors> ValidatePut(
            TKey key,
            TValue value,
            IStringMap<TKey> keyMap)
        {
            var errors = ValidationErrors.Empty;

            if (_prerequisites != null)
            {
                foreach(var prerequisite in _prerequisites)
                {
                    errors = errors.With(await prerequisite.ValidatePut(key, value, keyMap).ConfigureAwait(false));

                    if(errors.Any(e => e.IsFatal))
                    {
                        return errors;
                    }
                }
            }

            return errors.With(await ValidatePut(key, value, keyMap, errors).ConfigureAwait(false));
        }

        protected virtual async Task<ValidationErrors> ValidateDelete(
            TKey key,
            IStringMap<TKey> keyMap,
            ValidationErrors errors) => ValidationErrors.Empty;

        protected virtual async Task<ValidationErrors> ValidatePut(
            TKey key,
            TValue value,
            IStringMap<TKey> keyMap,
            ValidationErrors errors) => ValidationErrors.Empty;
    }



    public class C
    {
        public Guid CId { get; }

        public string Name { get; }
    }

    public class CValidator : ValidatorBase<Guid, C>
    {
        public CValidator(
            EntityValidator<Guid, C> entityValidator) : base(new[]
            {
                new EntityValidator<Guid, C>(e => e.CId)
            })
        {
        }

        protected override async Task<ValidationErrors> ValidatePut(
            Guid key, 
            C value, 
            IStringMap<Guid> keyMap, 
            ValidationErrors errors)
        {
            return errors
                .If(value.Name.StartsWith("X"), $"{nameof(value.Name)} can't start with 'X'.");
        }

        protected override Task<ValidationErrors> ValidateDelete(
            Guid key, 
            IStringMap<Guid> keyMap, 
            ValidationErrors errors)
        {

            return base.ValidateDelete(key, keyMap, errors);
        }
    }
}
