export const ACCOUNT_STATUS = Object.freeze({
  ACTIVE: 'active',
  DEAD: 'dead',
  INVALID: 'invalid',
  UNKNOWN: 'unknown',
});

export const SALE_STATUS = Object.freeze({
  UNSOLD: 'unsold',
  PENDING: 'pending_sale',
  SOLD: 'sold',
  RECYCLED: 'recycled',
});

export const CARD_STATUS = Object.freeze({
  AVAILABLE: 'available',
  DISABLED: 'disabled',
});

export const CARD_FAIL_TAG = Object.freeze({
  DECLINE: 'decline',
  THREE_DS: '3ds',
  INSUFFICIENT: 'insufficient',
});

export const PROXY_STATUS = Object.freeze({
  AVAILABLE: 'available',
  DISABLED: 'disabled',
});
